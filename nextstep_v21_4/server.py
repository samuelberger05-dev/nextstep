import os, re, sqlite3, secrets, hashlib, hmac, base64, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, urlencode, quote
import json

BASE = os.path.dirname(os.path.abspath(__file__))
# Production database: Render/Postgres supplies DATABASE_URL.
# Local development keeps using SQLite when DATABASE_URL is not configured.
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB = os.getenv("NEXTSTEP_DB_PATH", os.path.join(BASE, 'nextstep.db')).strip()
STATIC = os.path.join(BASE, 'static')
# Optional real AI tutor. Without an API key, NextStep keeps its local fallback tutor.
LLM_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
LLM_MODEL = os.getenv("OPENAI_MODEL", "gpt-6-luna").strip()
LLM_URL = os.getenv("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses").strip()
SESSIONS = {}

try:
    import psycopg
    from psycopg import errors as pg_errors
except ImportError:
    psycopg = None
    pg_errors = None


class HybridRow(dict):
    """A small compatibility row: supports both row['name'] and row[0]."""
    def __init__(self, columns, values):
        super().__init__(zip(columns, values))
        self._values = tuple(values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


def _replace_qmark_placeholders(sql):
    """Convert SQLite ? placeholders to psycopg %s without touching quoted strings."""
    out = []
    in_single = False
    in_double = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if ch == "'" and not in_double:
            if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                out.extend(["'", "'"])
                i += 2
                continue
            in_single = not in_single
            out.append(ch)
        elif ch == '"' and not in_single:
            if in_double and i + 1 < len(sql) and sql[i + 1] == '"':
                out.extend(['"', '"'])
                i += 2
                continue
            in_double = not in_double
            out.append(ch)
        elif ch == '?' and not in_single and not in_double:
            out.append('%s')
        else:
            out.append(ch)
        i += 1
    return ''.join(out)


_SERIAL_ID_TABLES = {
    'users', 'learning_tasks', 'tutor_messages', 'xp_events',
    'error_profiles', 'learning_competencies', 'curricula', 'competencies',
    'community_posts', 'community_comments', 'task_attempts'
}


def _translate_postgres_sql(sql):
    sql = _replace_qmark_placeholders(sql)
    # SQLite's autoincrement declaration -> PostgreSQL sequence-backed integer.
    sql = re.sub(r'\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b',
                 'SERIAL PRIMARY KEY', sql, flags=re.I)
    # SQLite date('now') idiom -> PostgreSQL standard current date.
    sql = re.sub(r"\bdate\(\s*'now'\s*\)", 'CURRENT_DATE', sql, flags=re.I)
    sql = re.sub(r"\bdate\(\s*created_at\s*\)", "created_at::date", sql, flags=re.I)

    upper = sql.lstrip().upper()
    if upper.startswith('INSERT OR IGNORE '):
        sql = re.sub(r'^\s*INSERT\s+OR\s+IGNORE\s+', 'INSERT ', sql, flags=re.I)
        if ' ON CONFLICT ' not in sql.upper():
            sql = sql.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'
    elif upper.startswith('INSERT OR REPLACE '):
        # The only current OR REPLACE usage is the session upsert.
        sql = re.sub(r'^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+sessions\s*\(\s*token\s*,\s*user_id\s*\)',
                     'INSERT INTO sessions(token,user_id)', sql, flags=re.I)
        sql = sql.rstrip().rstrip(';') + ' ON CONFLICT (token) DO UPDATE SET user_id=EXCLUDED.user_id'

    return sql


class PostgresCursor:
    def __init__(self, cursor):
        self._cursor = cursor
        self._lastrowid = None

    @property
    def lastrowid(self):
        return self._lastrowid

    def _wrap(self, raw):
        if raw is None:
            return None
        columns = [d.name for d in self._cursor.description]
        return HybridRow(columns, raw)

    def execute(self, sql, params=None):
        translated = _translate_postgres_sql(sql)
        self._lastrowid = None

        # Preserve the sqlite cursor.lastrowid behaviour used throughout NextStep.
        table_match = re.match(r'^\s*INSERT(?:\s+OR\s+(?:IGNORE|REPLACE))?\s+INTO\s+([A-Za-z_][A-Za-z0-9_]*)',
                               translated, flags=re.I)
        wants_id = bool(table_match and table_match.group(1).lower() in _SERIAL_ID_TABLES)
        if wants_id and ' RETURNING ' not in translated.upper():
            translated = translated.rstrip().rstrip(';') + ' RETURNING id'

        try:
            is_alter = translated.lstrip().upper().startswith('ALTER TABLE ')
            if is_alter:
                self._cursor.connection.execute('SAVEPOINT nextstep_alter')
            if params is None:
                self._cursor.execute(translated)
            else:
                self._cursor.execute(translated, params)
            if is_alter:
                self._cursor.connection.execute('RELEASE SAVEPOINT nextstep_alter')
        except Exception as exc:
            if pg_errors is not None and isinstance(exc, pg_errors.DuplicateColumn):
                # SQLite used to ignore "column already exists". In PostgreSQL the
                # failed statement aborts the transaction, so recover to a savepoint.
                try:
                    self._cursor.connection.execute('ROLLBACK TO SAVEPOINT nextstep_alter')
                    self._cursor.connection.execute('RELEASE SAVEPOINT nextstep_alter')
                except Exception:
                    pass
                raise sqlite3.OperationalError(str(exc)) from exc
            if pg_errors is not None and isinstance(exc, pg_errors.UniqueViolation):
                raise sqlite3.IntegrityError(str(exc)) from exc
            if is_alter:
                try:
                    self._cursor.connection.execute('ROLLBACK TO SAVEPOINT nextstep_alter')
                    self._cursor.connection.execute('RELEASE SAVEPOINT nextstep_alter')
                except Exception:
                    pass
            raise

        if wants_id:
            raw = self._cursor.fetchone()
            self._lastrowid = raw[0] if raw else None
        return self

    def executemany(self, sql, seq_of_params):
        translated = _translate_postgres_sql(sql)
        try:
            self._cursor.executemany(translated, seq_of_params)
        except Exception as exc:
            if pg_errors is not None and isinstance(exc, pg_errors.UniqueViolation):
                raise sqlite3.IntegrityError(str(exc)) from exc
            raise
        self._lastrowid = None
        return self

    def fetchone(self):
        return self._wrap(self._cursor.fetchone())

    def fetchall(self):
        return [self._wrap(row) for row in self._cursor.fetchall()]

    def __iter__(self):
        for row in self._cursor:
            yield self._wrap(row)


class PostgresConnection:
    def __init__(self, connection):
        self._connection = connection

    def execute(self, sql, params=None):
        return PostgresCursor(self._connection.cursor()).execute(sql, params)

    def executemany(self, sql, seq_of_params):
        return PostgresCursor(self._connection.cursor()).executemany(sql, seq_of_params)

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()


def db():
    if DATABASE_URL:
        if psycopg is None:
            raise RuntimeError('PostgreSQL ist konfiguriert, aber psycopg fehlt. requirements.txt muss psycopg[binary] enthalten.')
        return PostgresConnection(psycopg.connect(DATABASE_URL))
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    conn.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'both',
        country TEXT NOT NULL DEFAULT 'Österreich',
        language TEXT NOT NULL DEFAULT 'de',
        age_group TEXT NOT NULL DEFAULT '18–24',
        about TEXT NOT NULL DEFAULT '',
        goal TEXT NOT NULL DEFAULT '',
        category TEXT NOT NULL DEFAULT 'Sonstiges',
        level TEXT NOT NULL DEFAULT 'Anfänger',
        skills TEXT NOT NULL DEFAULT '',
        xp INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS learning_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL,
        module TEXT NOT NULL,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        task_type TEXT NOT NULL DEFAULT 'read',
        answer TEXT NOT NULL DEFAULT '',
        xp INTEGER NOT NULL DEFAULT 20,
        position INTEGER NOT NULL DEFAULT 0
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS user_task_progress (
        user_id INTEGER NOT NULL,
        task_id INTEGER NOT NULL,
        completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(user_id, task_id),
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(task_id) REFERENCES learning_tasks(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS tutor_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        role TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS tutor_profiles (
        user_id INTEGER PRIMARY KEY,
        subject TEXT NOT NULL DEFAULT 'Python',
        level TEXT NOT NULL DEFAULT 'Anfänger',
        current_topic TEXT NOT NULL DEFAULT 'Grundlagen',
        mastery INTEGER NOT NULL DEFAULT 0,
        diagnostic_step INTEGER NOT NULL DEFAULT 0,
        streak INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS xp_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        xp INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS learning_plans (
        user_id INTEGER PRIMARY KEY,
        goal TEXT NOT NULL,
        subject TEXT NOT NULL,
        target_level TEXT NOT NULL DEFAULT 'Fortgeschrittener Anfänger',
        current_level TEXT NOT NULL DEFAULT 'Anfänger',
        plan_json TEXT NOT NULL DEFAULT '[]',
        current_step INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    try: conn.execute("ALTER TABLE learning_plans ADD COLUMN learning_mode TEXT NOT NULL DEFAULT 'guided'")
    except sqlite3.OperationalError: pass
    try: conn.execute("ALTER TABLE learning_plans ADD COLUMN domain TEXT NOT NULL DEFAULT 'Allgemein'")
    except sqlite3.OperationalError: pass
    try: conn.execute("ALTER TABLE learning_plans ADD COLUMN target_level TEXT NOT NULL DEFAULT 'Fortgeschrittener Anfänger'")
    except sqlite3.OperationalError: pass
    conn.execute('''CREATE TABLE IF NOT EXISTS error_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        competency_position INTEGER NOT NULL,
        error_type TEXT NOT NULL,
        occurrences INTEGER NOT NULL DEFAULT 0,
        last_score INTEGER NOT NULL DEFAULT 0,
        last_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(user_id, competency_position, error_type),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute("CREATE TABLE IF NOT EXISTS learning_competencies (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, title TEXT NOT NULL, topic TEXT NOT NULL DEFAULT '', why TEXT NOT NULL DEFAULT '', position INTEGER NOT NULL, mastery INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'not_started', attempts INTEGER NOT NULL DEFAULT 0, correct_attempts INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(user_id, position), FOREIGN KEY(user_id) REFERENCES users(id))")
    conn.execute('''CREATE TABLE IF NOT EXISTS learning_preferences (
        user_id INTEGER PRIMARY KEY,
        domain TEXT NOT NULL DEFAULT 'Allgemein',
        current_level TEXT NOT NULL DEFAULT 'Anfänger',
        target_level TEXT NOT NULL DEFAULT 'Fortgeschrittener Anfänger',
        learning_mode TEXT NOT NULL DEFAULT 'guided',
        custom_goal TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS curricula (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country TEXT NOT NULL,
        school_type TEXT NOT NULL,
        year_level TEXT NOT NULL,
        subject TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT ''
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS competencies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        curriculum_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        difficulty INTEGER NOT NULL DEFAULT 1,
        position INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(curriculum_id) REFERENCES curricula(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS user_competencies (
        user_id INTEGER NOT NULL,
        competency_id INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'not_started',
        mastery INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(user_id, competency_id),
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(competency_id) REFERENCES competencies(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS user_curriculum (
        user_id INTEGER PRIMARY KEY,
        country TEXT NOT NULL DEFAULT 'Österreich',
        school_type TEXT NOT NULL DEFAULT 'Mittelschule',
        year_level TEXT NOT NULL DEFAULT '1. Klasse',
        subject TEXT NOT NULL DEFAULT 'Mathematik',
        curriculum_id INTEGER,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(curriculum_id) REFERENCES curricula(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS community_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        post_type TEXT NOT NULL DEFAULT 'question',
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        topic TEXT NOT NULL DEFAULT 'Allgemein',
        likes INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS community_comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        post_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        content TEXT NOT NULL,
        likes INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(post_id) REFERENCES community_posts(id),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS community_post_likes (
        user_id INTEGER NOT NULL,
        post_id INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(user_id, post_id),
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(post_id) REFERENCES community_posts(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS community_comment_likes (
        user_id INTEGER NOT NULL,
        comment_id INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(user_id, comment_id),
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(comment_id) REFERENCES community_comments(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS user_follows (
        follower_id INTEGER NOT NULL,
        following_id INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(follower_id, following_id),
        FOREIGN KEY(follower_id) REFERENCES users(id),
        FOREIGN KEY(following_id) REFERENCES users(id),
        CHECK(follower_id <> following_id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS user_interests (
        user_id INTEGER NOT NULL,
        interest TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(user_id, interest),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS user_learning_goals (
        user_id INTEGER NOT NULL,
        goal TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(user_id, goal),
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS github_connections (
        user_id INTEGER PRIMARY KEY,
        github_user_id INTEGER NOT NULL,
        login TEXT NOT NULL,
        name TEXT NOT NULL DEFAULT '',
        avatar_url TEXT NOT NULL DEFAULT '',
        html_url TEXT NOT NULL DEFAULT '',
        access_token TEXT NOT NULL,
        refresh_token TEXT NOT NULL DEFAULT '',
        expires_at INTEGER,
        scope TEXT NOT NULL DEFAULT '',
        public_repos INTEGER NOT NULL DEFAULT 0,
        languages_json TEXT NOT NULL DEFAULT '[]',
        repos_json TEXT NOT NULL DEFAULT '[]',
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS reddit_connections (
        user_id INTEGER PRIMARY KEY,
        reddit_user_id TEXT NOT NULL DEFAULT '',
        username TEXT NOT NULL,
        icon_img TEXT NOT NULL DEFAULT '',
        access_token TEXT NOT NULL,
        refresh_token TEXT NOT NULL DEFAULT '',
        expires_at INTEGER,
        scope TEXT NOT NULL DEFAULT '',
        communities_json TEXT NOT NULL DEFAULT '[]',
        use_for_matching INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )''')
    conn.execute('''CREATE TABLE IF NOT EXISTS task_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        task_id INTEGER NOT NULL,
        answer TEXT NOT NULL,
        correct INTEGER NOT NULL DEFAULT 0,
        feedback TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(task_id) REFERENCES learning_tasks(id)
    )''')
    for sql in [
        "ALTER TABLE task_attempts ADD COLUMN error_type TEXT NOT NULL DEFAULT 'none'",
        "ALTER TABLE task_attempts ADD COLUMN score INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE task_attempts ADD COLUMN next_hint TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE task_attempts ADD COLUMN remediation TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE learning_tasks ADD COLUMN generated_for_user INTEGER",
        "ALTER TABLE learning_tasks ADD COLUMN generated_competency_position INTEGER",
        "ALTER TABLE learning_tasks ADD COLUMN generated INTEGER NOT NULL DEFAULT 0"
    ]:
        try: conn.execute(sql)
        except sqlite3.OperationalError: pass
    try:
        conn.execute("ALTER TABLE tutor_profiles ADD COLUMN diagnostic_step INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    seed = [
        # Python
        ('Python für Anfänger','1 · Grundlagen','Was ist eine Variable?', 'Eine Variable ist ein Name, unter dem du einen Wert speicherst. Beispiel: name = "Kobold".', 'read', '', 10, 1),
        ('Python für Anfänger','1 · Grundlagen','Deine erste Ausgabe', 'Mit print() kannst du etwas auf dem Bildschirm ausgeben.', 'code', 'print("Hallo Welt!")', 20, 2),
        ('Python für Anfänger','1 · Grundlagen','Variablen verwenden', 'Speichere deinen Namen in name und gib ihn anschließend aus.', 'code', 'name + print', 25, 3),
        ('Python für Anfänger','2 · Logik','Bedingungen', 'Mit if kannst du abhängig von einer Bedingung Code ausführen.', 'code', 'if alter >= 18', 20, 4),
        ('Python für Anfänger','2 · Logik','Schleifen', 'Mit einer for-Schleife kannst du eine Aktion mehrfach ausführen.', 'code', 'for', 30, 5),
        ('Python für Anfänger','3 · Praxis','Mini-Projekt', 'Baue einen kleinen Rechner, der zwei Zahlen addiert und das Ergebnis ausgibt.', 'project', 'print +', 50, 6),
        # Mathematik
        ('Mathematik für Anfänger','1 · Grundlagen','Rechnen mit Variablen', 'Wenn x = 4 gilt: Was ist 2x + 3?', 'math', '11', 20, 101),
        ('Mathematik für Anfänger','1 · Grundlagen','Brüche verstehen', 'Erkläre mit eigenen Worten, was 1/2 bedeutet.', 'text', 'hälfte', 20, 102),
        ('Mathematik für Anfänger','2 · Algebra','Lineare Gleichungen', 'Löse: 2x + 4 = 10.', 'math', '3', 30, 103),
        ('Mathematik für Anfänger','2 · Algebra','Prozentrechnung', 'Wie viel sind 20 % von 50?', 'math', '10', 25, 104),
        ('Mathematik für Anfänger','3 · Praxis','Anwendungsproblem', 'Ein Produkt kostet 80 €. Es wird um 25 % reduziert. Wie hoch ist der neue Preis?', 'math', '60', 40, 105),
        # Englisch
        ('Englisch für Anfänger','1 · Grundlagen','Vorstellen', 'Übersetze: „Ich heiße Anna und ich lerne Englisch.“', 'text', 'my name', 20, 201),
        ('Englisch für Anfänger','1 · Grundlagen','Simple Present', 'Bilde einen englischen Satz mit „I play“.', 'text', 'i play', 20, 202),
        ('Englisch für Anfänger','2 · Wortschatz','Alltag', 'Nenne fünf englische Wörter aus dem Alltag und ihre deutsche Bedeutung.', 'text', '5', 25, 203),
        ('Englisch für Anfänger','2 · Grammatik','Fragen bilden', 'Bilde eine englische Frage mit „do“ und „you“.', 'text', 'do you', 30, 204),
        ('Englisch für Anfänger','3 · Praxis','Mini-Dialog', 'Schreibe einen kurzen englischen Dialog mit mindestens vier Sätzen.', 'text', 'dialog', 40, 205),
        # Physik
        ('Physik für Anfänger','1 · Grundlagen','Kraft und Bewegung', 'Erkläre mit eigenen Worten, was eine Kraft bewirken kann.', 'text', 'bewegung', 20, 301),
        ('Physik für Anfänger','1 · Grundlagen','Geschwindigkeit', 'Ein Auto fährt 100 km in 2 Stunden. Wie hoch ist die Durchschnittsgeschwindigkeit?', 'math', '50', 25, 302),
        ('Physik für Anfänger','2 · Energie','Energieformen', 'Nenne drei verschiedene Energieformen.', 'text', '3', 25, 303),
        ('Physik für Anfänger','2 · Elektrizität','Stromkreis', 'Welche drei grundlegenden Bestandteile braucht ein einfacher Stromkreis?', 'text', 'quelle', 30, 304),        ('Physik für Anfänger','3 · Praxis','Anwendung', 'Erkläre ein physikalisches Phänomen aus deinem Alltag.', 'text', 'erklärung', 40, 305),
        # Chemie
        ('Chemie für Anfänger','1 · Grundlagen','Atome', 'Erkläre mit eigenen Worten, was ein Atom ist.', 'text', 'klein', 20, 401),
        ('Chemie für Anfänger','1 · Grundlagen','Elemente', 'Was ist ein chemisches Element?', 'text', 'atomsorte', 20, 402),
        ('Chemie für Anfänger','2 · Reaktionen','Chemische Reaktion', 'Nenne ein Beispiel für eine chemische Reaktion aus dem Alltag.', 'text', 'reaktion', 25, 403),
        ('Chemie für Anfänger','2 · Stoffe','Mischen und Trennen', 'Nenne eine Methode, mit der man Stoffgemische trennen kann.', 'text', 'filtr', 30, 404),
        ('Chemie für Anfänger','3 · Praxis','Alltagschemie', 'Erkläre ein chemisches Phänomen, das du aus deinem Alltag kennst.', 'text', 'erklärung', 40, 405),
    ]
    for item in seed:
        exists = conn.execute('SELECT id FROM learning_tasks WHERE path=? AND position=?', (item[0],item[-1])).fetchone()
        if not exists:
            conn.execute('INSERT INTO learning_tasks(path,module,title,content,task_type,answer,xp,position) VALUES(?,?,?,?,?,?,?,?)', item)
    advanced=[
        ('Python – Fortgeschritten','Abstraktion','Funktionen entwerfen','Schreibe eine Funktion `mittelwert(zahlen)`, die den Durchschnitt einer Liste zurückgibt.','code','def mittelwert',40,1001),
        ('Python – Fortgeschritten','Algorithmen','Datenstrukturen vergleichen','Erkläre, wann ein Dictionary gegenüber einer Liste sinnvoller sein kann.','text','dictionary',40,1002),
        ('Python – Fortgeschritten','Architektur','Klassen modellieren','Erkläre, warum man eine Klasse verwenden könnte, wenn ein Programm größer wird.','text','klasse',45,1003),
        ('Mathematik – Fortgeschritten','Analysis','Ableitung verstehen','Erkläre mit eigenen Worten, was die Ableitung einer Funktion an einer Stelle beschreibt.','text','änderung',40,1101),
        ('Mathematik – Fortgeschritten','Lineare Algebra','Vektoren','Berechne das Skalarprodukt von (2,3) und (4,1).','math','11',40,1102),
        ('Mathematik – Fortgeschritten','Beweise','Mathematisch argumentieren','Erkläre, was eine mathematische Aussage von einer bloßen Vermutung unterscheidet.','text','beweis',45,1103),
    ]
    for item in advanced:
        exists=conn.execute('SELECT id FROM learning_tasks WHERE path=? AND position=?',(item[0],item[-1])).fetchone()
        if not exists: conn.execute('INSERT INTO learning_tasks(path,module,title,content,task_type,answer,xp,position) VALUES(?,?,?,?,?,?,?,?)',item)
    try:
        conn.execute("ALTER TABLE users ADD COLUMN language TEXT NOT NULL DEFAULT 'de'")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    # Curriculum Engine: Österreichische Sekundarstufe I als strukturierte Startbasis.
    # Die Themen sind bewusst als Kompetenz-Map modelliert und später erweiterbar.
    curriculum_data = {
      'Mathematik': {
        '1. Klasse':['Zahlen und Rechenoperationen','Brüche und Dezimalzahlen','Terme und einfache Gleichungen','Geometrische Grundbegriffe','Daten und Diagramme'],
        '2. Klasse':['Prozent- und Zinsrechnung','Terme und lineare Gleichungen','Geometrie und Flächen','Zuordnungen und Funktionen','Statistik und Wahrscheinlichkeit'],
        '3. Klasse':['Lineare Funktionen','Gleichungssysteme','Potenzen und Wurzeln','Geometrische Körper','Wahrscheinlichkeit und Statistik'],
        '4. Klasse':['Quadratische Zusammenhänge','Funktionen und Modelle','Trigonometrische Grundlagen','Analytische Geometrie','Anwendungsorientierte Mathematik']},
      'Deutsch': {
        '1. Klasse':['Lesen und Textverständnis','Schreiben und Erzählen','Rechtschreibung und Grammatik','Sprechen und Zuhören','Medien und Informationssuche'],
        '2. Klasse':['Sachtexte verstehen','Argumentieren und Begründen','Satzbau und Wortarten','Literarische Texte','Präsentieren und Medien'],
        '3. Klasse':['Erörterung und Argumentation','Literarische Analyse','Grammatik und Stil','Recherche und Quellen','Kommunikation und Medien'],
        '4. Klasse':['Komplexe Textsorten','Interpretation','Argumentatives Schreiben','Sprachreflexion','Präsentation und Prüfungskommunikation']},
      'Englisch': {
        '1. Klasse':['Alltag und Vorstellen','Grundlegende Grammatik','Wortschatz und Aussprache','Lesen und Hörverstehen','Kurze Texte schreiben'],
        '2. Klasse':['Alltagskommunikation','Simple Present und Past','Fragen und Dialoge','Lesen und Hörverstehen','Einfache Texte verfassen'],
        '3. Klasse':['Zeitformen erweitern','Meinungen und Begründungen','Längere Texte verstehen','Schreiben und Präsentieren','Interkulturelle Kommunikation'],
        '4. Klasse':['Komplexere Texte','Argumentieren auf Englisch','Grammatik sicher anwenden','Präsentationen und Diskussionen','Selbstständiges Schreiben']},
      'Physik': {
        '1. Klasse':['Beobachten und Messen','Kraft und Bewegung','Energie im Alltag','Wärme und Temperatur','Einfache technische Anwendungen'],
        '2. Klasse':['Mechanik','Druck und Flüssigkeiten','Elektrizität','Optik','Energieumwandlungen'],
        '3. Klasse':['Elektrische Größen','Magnetismus','Wellen und Schall','Wärmelehre','Technische Anwendungen'],
        '4. Klasse':['Bewegung und Kräfte vertiefen','Elektrische Schaltungen','Optische Systeme','Energie und Umwelt','Physikalische Modelle und Experimente']},
      'Chemie': {
        '1. Klasse':['Stoffe und Eigenschaften','Teilchenmodell','Mischen und Trennen','Sicherheit im Labor','Chemie im Alltag'],
        '2. Klasse':['Atome und Elemente','Periodensystem','Chemische Bindungen','Chemische Reaktionen','Säuren und Basen'],
        '3. Klasse':['Reaktionsgleichungen','Stoffmengen und Modelle','Säure-Base-Reaktionen','Redoxreaktionen','Organische Stoffe im Alltag'],
        '4. Klasse':['Organische Chemie','Kohlenstoffverbindungen','Chemische Gleichgewichte','Umweltchemie','Anwendungen und Experimente']}
    }
    for school in ['Mittelschule','AHS-Unterstufe']:
      for subject, years in curriculum_data.items():
        for year, topics in years.items():
          row=conn.execute('SELECT id FROM curricula WHERE country=? AND school_type=? AND year_level=? AND subject=?',(
            'Österreich',school,year,subject)).fetchone()
          if row: cid=row['id']
          else:
            cur=conn.execute('INSERT INTO curricula(country,school_type,year_level,subject,title,description) VALUES(?,?,?,?,?,?)',
              ('Österreich',school,year,subject,f'{subject} – {year}',f'Kompetenzorientierter Lernpfad für {school}, {year} in Österreich.'))
            cid=cur.lastrowid
          count=conn.execute('SELECT COUNT(*) FROM competencies WHERE curriculum_id=?',(cid,)).fetchone()[0]
          if count==0:
            for pos,topic in enumerate(topics,1):
              conn.execute('INSERT INTO competencies(curriculum_id,title,description,difficulty,position) VALUES(?,?,?,?,?)',
                (cid,topic,f'Du sollst {topic.lower()} verstehen, anwenden und an einer Aufgabe zeigen können.',1 if pos<=2 else 2,pos))
    conn.commit(); conn.close()


def hash_password(password):
    salt = secrets.token_bytes(16)
    rounds = 600_000
    digest = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, rounds)
    return f'pbkdf2_sha256${rounds}${base64.b64encode(salt).decode()}${base64.b64encode(digest).decode()}'


def verify_password(password, stored):
    try:
        algo, rounds, salt_b64, digest_b64 = stored.split('$')
        if algo != 'pbkdf2_sha256': return False
        digest = hashlib.pbkdf2_hmac('sha256', password.encode(), base64.b64decode(salt_b64), int(rounds))
        return hmac.compare_digest(digest, base64.b64decode(digest_b64))
    except Exception:
        return False


def clean(s, maxlen=5000):
    return str(s or '').strip()[:maxlen]


def json_body(handler):
    length = int(handler.headers.get('Content-Length', '0'))
    if length > 100_000: raise ValueError('request too large')
    raw = handler.rfile.read(length)
    return json.loads(raw.decode('utf-8') or '{}')


def session_user(handler):
    cookie = handler.headers.get('Cookie', '')
    token = None
    for part in cookie.split(';'):
        if part.strip().startswith('session='):
            token = part.strip().split('=',1)[1]
    if not token:
        return None
    # Keep sessions in the database so a Render restart does not log everyone out.
    conn = db()
    session = conn.execute('SELECT user_id FROM sessions WHERE token=?', (token,)).fetchone()
    if not session:
        conn.close()
        # Backward compatibility with an already-running prototype session.
        uid = SESSIONS.get(token)
        if not uid:
            return None
        row = db().execute('SELECT * FROM users WHERE id=?', (uid,)).fetchone()
        return row
    row = conn.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
    conn.close()
    return row


def split_tags(value, max_items=20, max_len=60):
    if isinstance(value, list):
        raw=value
    else:
        raw=str(value or '').replace(';', ',').split(',')
    out=[]
    for item in raw:
        item=clean(item,max_len)
        if item and item.lower() not in {x.lower() for x in out}: out.append(item)
        if len(out)>=max_items: break
    return out

def set_profile_tags(conn, user_id, interests=None, learning_goals=None):
    if interests is not None:
        conn.execute('DELETE FROM user_interests WHERE user_id=?',(user_id,))
        conn.executemany('INSERT OR IGNORE INTO user_interests(user_id,interest) VALUES(?,?)',[(user_id,x) for x in split_tags(interests)])
    if learning_goals is not None:
        conn.execute('DELETE FROM user_learning_goals WHERE user_id=?',(user_id,))
        conn.executemany('INSERT OR IGNORE INTO user_learning_goals(user_id,goal) VALUES(?,?)',[(user_id,x) for x in split_tags(learning_goals)])

def profile_payload(conn, user_id):
    row=conn.execute('SELECT * FROM users WHERE id=?',(user_id,)).fetchone()
    if not row: return None
    d=public_user(row)
    d['skills_list']=split_tags(row['skills'])
    d['interests']=[r['interest'] for r in conn.execute('SELECT interest FROM user_interests WHERE user_id=? ORDER BY interest',(user_id,)).fetchall()]
    d['learning_goals']=[r['goal'] for r in conn.execute('SELECT goal FROM user_learning_goals WHERE user_id=? ORDER BY goal',(user_id,)).fetchall()]
    d['follower_count']=conn.execute('SELECT COUNT(*) FROM user_follows WHERE following_id=?',(user_id,)).fetchone()[0]
    d['following_count']=conn.execute('SELECT COUNT(*) FROM user_follows WHERE follower_id=?',(user_id,)).fetchone()[0]
    return d

def normalized_tokens(values):
    """Normalize profile/external signals into comparable lowercase tokens."""
    text=' '.join(str(v or '') for v in (values or [])).lower()
    text=text.replace('c++','cpp').replace('c#','csharp').replace('.net','dotnet').replace('machine-learning','machine learning')
    return {x for x in re.findall(r'[a-zäöüß0-9+#.-]{3,}', text) if x not in {
        'und','oder','für','ich','mit','the','and','you','bei','von','der','die','das','ein','eine','ist','auf','zu','den','des'
    }}

# Small, explicit aliases make the first version useful without pretending to be an AI semantic model.
MATCH_ALIASES={
    'ml': {'machinelearning'},
    'machinelearning': {'ml'},
    'ai': {'artificialintelligence'},
    'artificialintelligence': {'ai'},
    'javascript': {'js'},
    'js': {'javascript'},
    'programming': {'coding','programmieren'},
    'coding': {'programming','programmieren'},
    'programmieren': {'programming','coding'},
    'fotografie': {'photography'},
    'photography': {'fotografie'},
    'psychology': {'psychologie'},
    'psychologie': {'psychology'},
    'music': {'musik'},
    'musik': {'music'},
}

def expanded_tokens(values):
    base=normalized_tokens(values)
    expanded=set(base)
    for token in list(base):
        expanded.update(MATCH_ALIASES.get(token, set()))
    return expanded

def github_signals(conn, user_id):
    row=conn.execute('SELECT languages_json,repos_json,public_repos,login FROM github_connections WHERE user_id=?',(user_id,)).fetchone()
    if not row:
        return {'languages':set(),'match_languages':set(),'topics':set(),'repo_names':set(),'descriptions':set(),'login':'','repos':0}
    try: languages={str(x[0]).lower() for x in json.loads(row['languages_json'] or '[]') if isinstance(x,list) and x}
    except Exception: languages=set()
    try: repos=json.loads(row['repos_json'] or '[]')
    except Exception: repos=[]
    topics=set(); repo_names=set(); descriptions=[]
    for repo in repos if isinstance(repos,list) else []:
        repo_names.update(expanded_tokens([repo.get('name','')]))
        descriptions.append(repo.get('description') or '')
        topics.update(expanded_tokens(repo.get('topics',[])))
    return {'languages':languages,'match_languages':expanded_tokens(languages),'topics':topics,'repo_names':repo_names,'descriptions':expanded_tokens(descriptions),'login':row['login'] or '','repos':int(row['public_repos'] or 0)}

def match_token_overlap(a,b):
    return a & b

def matching_users(conn, user_id, limit=12):
    me=profile_payload(conn,user_id)
    if not me: return []
    my_profile=expanded_tokens(me['skills_list']+me['interests']+me['learning_goals']+[me.get('category','')])
    my_skills=expanded_tokens(me['skills_list'])
    my_interests=expanded_tokens(me['interests'])
    my_learning=expanded_tokens(me['learning_goals'])
    my_github=github_signals(conn,user_id)
    my_reddit=reddit_selected_communities(conn,user_id)
    rows=conn.execute('SELECT id,name,level,category,skills,goal,about,xp FROM users WHERE id<>?',(user_id,)).fetchall()
    following={r['following_id'] for r in conn.execute('SELECT following_id FROM user_follows WHERE follower_id=?',(user_id,)).fetchall()}
    out=[]
    for r in rows:
        other=profile_payload(conn,r['id'])
        their_profile=expanded_tokens(other['skills_list']+other['interests']+other['learning_goals']+[other.get('category','')])
        their_skills=expanded_tokens(other['skills_list'])
        their_interests=expanded_tokens(other['interests'])
        their_learning=expanded_tokens(other['learning_goals'])
        their_github=github_signals(conn,r['id'])
        their_reddit=reddit_selected_communities(conn,r['id'])

        # Directional learning value: what they can teach you and what you can teach them.
        can_help_me=match_token_overlap(my_learning, their_skills | their_github['match_languages'] | their_github['topics'])
        i_can_help=match_token_overlap(their_learning, my_skills | my_github['match_languages'] | my_github['topics'])
        shared_interest=match_token_overlap(my_interests, their_interests)
        shared_profile=match_token_overlap(my_profile, their_profile)
        github_learning=match_token_overlap(my_learning, their_github['match_languages'] | their_github['topics'] | their_github['repo_names'])
        github_teaching=match_token_overlap(their_learning, my_github['match_languages'] | my_github['topics'] | my_github['repo_names'])
        reddit_match=my_reddit & their_reddit
        category_bonus=me.get('category') and other.get('category') and me['category'].strip().lower()==other['category'].strip().lower()

        # Weights intentionally favor reciprocal learning over superficial profile overlap.
        score=(len(can_help_me)*12 + len(i_can_help)*10 + len(shared_interest)*5 +
               len(shared_profile)*2 + len(github_learning)*5 + len(github_teaching)*4 +
               len(reddit_match)*7 + (4 if category_bonus else 0) + (1 if r['id'] in following else 0))
        if score<=0: continue

        reasons=[]
        if can_help_me: reasons.append('kann dir bei '+', '.join(sorted(can_help_me)[:3])+' helfen')
        if i_can_help: reasons.append('du kannst bei '+', '.join(sorted(i_can_help)[:3])+' helfen')
        if shared_interest: reasons.append('gemeinsames Interesse: '+', '.join(sorted(shared_interest)[:3]))
        if github_learning: reasons.append('GitHub passt zu deinem Lernziel: '+', '.join(sorted(github_learning)[:3]))
        if github_teaching: reasons.append('dein GitHub-Wissen passt zu seinem Lernziel')
        if reddit_match: reasons.append('gemeinsame Reddit-Community: '+', '.join(sorted(reddit_match)[:3]))
        if category_bonus: reasons.append('gleicher Themenbereich')

        signal_counts={
            'can_help_me':len(can_help_me),'i_can_help':len(i_can_help),'shared_interests':len(shared_interest),
            'github_learning':len(github_learning),'github_teaching':len(github_teaching),'reddit_shared':len(reddit_match)
        }
        out.append({'id':r['id'],'name':r['name'],'level':r['level'],'category':r['category'],'xp':r['xp'],
            'following':r['id'] in following,'score':score,'reasons':reasons[:4],
            'skills':other['skills_list'],'interests':other['interests'],'learning_goals':other['learning_goals'],
            'github_languages':sorted(their_github['languages'])[:12],'github_login':their_github['login'],
            'github_repos':their_github['repos'],'reddit_shared':sorted(reddit_match)[:8], 'signals':signal_counts})
    out.sort(key=lambda x:(-x['score'],-x['signals']['can_help_me'],-x['signals']['i_can_help'],-x['xp']))
    return out[:limit]

def public_user(row):
    if not row: return None
    d = dict(row); d.pop('password_hash', None); d['email'] = d['email']
    return d


def is_advanced_target(target_level):
    return str(target_level or '').lower() in ['fortgeschritten','universitätsniveau','experte','advanced']

def build_plan(row, target_level=None, custom_goal=None):
    goal=(custom_goal if custom_goal is not None else row['goal'] or '').lower()
    category=(row['category'] or '').lower()
    text=goal+' '+category
    level=(target_level or row['level'] or 'Anfänger').lower()
    advanced=level in ['fortgeschritten','universitätsniveau','experte','advanced']
    if 'python' in text or 'programm' in text or 'technologie' in text:
        subject='Python'
        if advanced:
            path='Python – Fortgeschritten'
            topics=[('Abstraktion','Funktionen, Module & saubere Schnittstellen','Du strukturierst Programme so, dass sie wachsen können.'),('Algorithmen','Datenstrukturen & Laufzeit','Du lernst, Lösungen systematisch zu vergleichen.'),('Architektur','Objektorientierung & Design','Du entwickelst größere Programme mit klaren Strukturen.'),('Praxis','Eigenes anspruchsvolles Projekt','Du beweist dein Können mit einem realen Projekt.')]
        else:
            path='Python für Anfänger'
            topics=[('Grundlagen verstehen','Variablen & Ausgabe','Du baust ein stabiles Programmierfundament.'),('Logik beherrschen','Bedingungen & Schleifen','Du lernst, Programme Entscheidungen und Wiederholungen ausführen zu lassen.'),('Praxis','Eigenes Mini-Projekt','Du zeigst dein Können mit einem kleinen Programm.')]
    elif 'mathe' in text or 'mathemat' in text:
        subject='Mathematik'
        if advanced:
            path='Mathematik – Fortgeschritten'
            topics=[('Analysis','Grenzwerte & Ableitungen','Du untersuchst Funktionen und Veränderungen.'),('Lineare Algebra','Vektoren & Matrizen','Du beschreibst mathematische Strukturen systematisch.'),('Beweise','Logik & mathematische Argumentation','Du lernst, Aussagen sauber zu begründen.'),('Praxis','Modellierungsprojekt','Du setzt Mathematik auf ein reales Problem an.')]
        else:
            path='Mathematik für Anfänger'
            topics=[('Grundlagen verstehen','Rechnen & Brüche','Du baust Sicherheit bei den wichtigsten Grundlagen auf.'),('Algebra anwenden','Gleichungen & Prozentrechnung','Du lernst mathematische Probleme systematisch zu lösen.'),('Praxis','Anwendungsprobleme','Du überträgst Mathematik auf echte Situationen.')]
    elif 'englisch' in text or 'english' in text or 'sprache' in text:
        subject='Englisch'; path='Englisch für Anfänger'; topics=[('Grundlagen','Vorstellen & einfache Sätze','Du baust einen praktischen Grundwortschatz auf.'),('Grammatik & Wortschatz','Alltagssprache','Du kombinierst Wörter zu korrekten Sätzen.'),('Praxis','Dialoge','Du verwendest Englisch in echten Gesprächssituationen.')]
    elif 'physik' in text:
        subject='Physik'; path='Physik für Anfänger'; topics=[('Grundlagen','Kraft & Bewegung','Du verstehst grundlegende physikalische Größen.'),('Energie & Elektrizität','Modelle verstehen','Du verbindest Formeln mit realen Phänomenen.'),('Praxis','Alltagsphysik','Du erklärst Physik anhand konkreter Situationen.')]
    elif 'chemie' in text:
        subject='Chemie'; path='Chemie für Anfänger'; topics=[('Grundlagen','Atome & Elemente','Du lernst, aus welchen Bausteinen Stoffe bestehen.'),('Reaktionen','Stoffe verändern sich','Du verstehst grundlegende chemische Vorgänge.'),('Praxis','Chemie im Alltag','Du erkennst Chemie in deiner Umgebung.')]
    else:
        subject=row['category'] or 'Lernen'; path='Individueller Lernpfad'; topics=[('Ziel klären','Grundlagen','Wir klären zuerst, was du bereits kannst und was du erreichen möchtest.'),('Anwenden','Praxis','Du löst Aufgaben, die zu deinem aktuellen Niveau passen.'),('Kompetenz beweisen','Projekt','Du zeigst dein Können mit einem eigenen Ergebnis.')]
    return subject, path, [{'title':t,'topic':tp,'why':w,'step':i} for i,(t,tp,w) in enumerate(topics,1)]

def sync_plan_competencies(conn,user_id,steps):
    for step in steps:
        conn.execute("INSERT INTO learning_competencies(user_id,title,topic,why,position) VALUES(?,?,?,?,?) ON CONFLICT(user_id,position) DO UPDATE SET title=excluded.title,topic=excluded.topic,why=excluded.why",(user_id,step['title'],step.get('topic',''),step.get('why',''),step['step']))
    if steps: conn.execute('DELETE FROM learning_competencies WHERE user_id=? AND position>?',(user_id,max(x['step'] for x in steps)))
def competency_index_for_task(task,tasks,step_count):
    if not step_count:return 1
    positions=sorted(int(t['position']) for t in tasks)
    try: idx=positions.index(int(task['position']))
    except ValueError: idx=0
    return min(step_count,(idx*step_count)//max(1,len(positions))+1)
def update_learning_competency(conn,user_id,position,correct):
    c=conn.execute('SELECT * FROM learning_competencies WHERE user_id=? AND position=?',(user_id,position)).fetchone()
    if not c:return None
    attempts=c['attempts']+1; corrects=c['correct_attempts']+(1 if correct else 0); mastery=max(0,min(100,c['mastery']+(35 if correct else -10)))
    status='mastered' if mastery>=80 else 'in_progress'
    conn.execute('UPDATE learning_competencies SET mastery=?,status=?,attempts=?,correct_attempts=?,updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND position=?',(mastery,status,attempts,corrects,user_id,position))
    return conn.execute('SELECT * FROM learning_competencies WHERE user_id=? AND position=?',(user_id,position)).fetchone()
def next_learning_competency(conn,user_id):
    return conn.execute("SELECT * FROM learning_competencies WHERE user_id=? AND status!='mastered' ORDER BY position LIMIT 1",(user_id,)).fetchone()

def llm_enabled():
    return bool(LLM_API_KEY)


def llm_text(instructions, payload, max_output=900):
    """Call the configured Responses API and return output_text, or None on failure."""
    if not llm_enabled():
        return None
    body = {
        "model": LLM_MODEL,
        "instructions": instructions,
        "input": json.dumps(payload, ensure_ascii=False),
        "max_output_tokens": max_output,
    }
    req = urllib.request.Request(
        LLM_URL,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": "Bearer " + LLM_API_KEY, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data=json.loads(resp.read().decode("utf-8"))
        return (data.get("output_text") or "").strip() or None
    except Exception:
        return None


def parse_json_object(text):
    if not text: return None
    text=text.strip()
    try: return json.loads(text)
    except Exception: pass
    m=re.search(r"\\{.*\\}", text, re.S)
    if not m: return None
    try: return json.loads(m.group(0))
    except Exception: return None


def llm_generate_task(user, comp, plan, recent_attempts):
    payload={
        "goal": plan["goal"], "subject": plan["subject"],
        "current_level": plan["current_level"], "target_level": plan["target_level"],
        "learning_mode": plan["learning_mode"],
        "competency": {"title": comp["title"], "topic": comp["topic"], "mastery": comp["mastery"], "attempts": comp["attempts"]},
        "recent_attempts": (recent_attempts.get("attempts", [])[-5:] if isinstance(recent_attempts, dict) else recent_attempts[-5:]), "error_profile": (recent_attempts.get("error_profile", []) if isinstance(recent_attempts, dict) else [])
    }
    instructions=("Du bist der adaptive NextStep-Lerntutor. Erzeuge genau EINE neue Lernaufgabe, "
        "die zur Kompetenz, zum Lernziel und zum aktuellen Niveau passt. Sie muss das Verständnis "
        "testen, nicht bloß Fakten abfragen. Nutze frühere Fehler, wenn vorhanden. Keine Lösung im Aufgabentext. "
        "Antworte ausschließlich als gültiges JSON mit den Feldern title, content, task_type, answer, feedback_hint, xp. "
        "task_type muss code, math oder text sein. answer ist eine kurze Musterlösung oder Bewertungsreferenz. "
        "xp muss zwischen 15 und 60 liegen. feedback_hint beschreibt, worauf die Bewertung achten soll.")
    raw=llm_text(instructions,payload,1000)
    obj=parse_json_object(raw)
    if not obj or not obj.get("title") or not obj.get("content") or not obj.get("answer"):
        return None
    obj["task_type"]=obj.get("task_type") if obj.get("task_type") in ("code","math","text") else "text"
    try: obj["xp"]=max(15,min(60,int(obj.get("xp",30))))
    except Exception: obj["xp"]=30
    return obj


def llm_evaluate_answer(task, answer, comp, plan):
    payload={"task": {"title":task["title"],"content":task["content"],"type":task["task_type"],"reference_answer":task["answer"],"feedback_hint":task["feedback_hint"] if "feedback_hint" in task.keys() else ""},
             "student_answer":answer,"competency":dict(comp),"subject":plan["subject"]}
    instructions=("Bewerte die Schülerantwort als fairer Tutor. Entscheide, ob die Kompetenz in dieser Aufgabe "
        "nachgewiesen wurde. Teilweise richtige Antworten sollen nicht automatisch als falsch gelten. "
        "Antworte ausschließlich als JSON: correct (boolean), score (0-100), feedback (string, auf Deutsch), "
        "error_type (one of none, concept, calculation, syntax, incomplete, unclear), next_hint (string). "
        "Gib bei einer falschen Antwort einen konkreten Hinweis, aber nicht sofort die komplette Lösung.")
    raw=llm_text(instructions,payload,700)
    obj=parse_json_object(raw)
    if not obj or "correct" not in obj or not obj.get("feedback"):
        return None
    obj["correct"]=bool(obj["correct"]); obj["score"]=max(0,min(100,int(obj.get("score",100 if obj["correct"] else 0))))
    return obj


def local_diagnostic_evaluation(task, answer, comp):
    """Small deterministic diagnostic layer used when no LLM is configured."""
    a=answer.strip().lower()
    content=(task['content'] or '').lower()
    if not a:
        return {'correct':False,'score':0,'feedback':'Bitte gib zuerst deine eigene Lösung ein.','error_type':'incomplete','next_hint':'Formuliere wenigstens einen ersten Lösungsversuch.','remediation':'Starte mit dem kleinsten Teil der Aufgabe und schreibe auf, was du bereits weißt.'}
    # Python basics
    if 'python' in (comp['title'] or '').lower() or 'python' in (comp['topic'] or '').lower() or task['task_type']=='code':
        if 'zahl = 7' in content:
            ok=('zahl' in a and '=' in a and '7' in a and 'print' in a)
            return {'correct':ok,'score':100 if ok else 30,'feedback':'✅ Du hast Variable und Ausgabe korrekt verbunden.' if ok else '💡 Du hast die richtige Richtung, aber Variable und Ausgabe müssen noch sauber verbunden werden.','error_type':'none' if ok else ('syntax' if 'print' not in a or '=' not in a else 'concept'),'next_hint':'Lege zuerst `zahl = 7` an und gib danach `zahl` mit `print()` aus.','remediation':'Merke dir das Muster: Wert speichern → Variable verwenden → Ergebnis ausgeben.'}
        if 'gesamtpreis' in content:
            ok=('preis' in a and 'menge' in a and 'print' in a and ('*' in a or 'preis * menge' in a))
            return {'correct':ok,'score':100 if ok else 35,'feedback':'✅ Genau: Du kombinierst Variablen und Multiplikation.' if ok else '💡 Der fehlende Schritt ist die Multiplikation von Preis und Menge.','error_type':'none' if ok else 'concept','next_hint':'Überlege, welche Rechenoperation den Gesamtpreis aus Preis und Menge ergibt.','remediation':'Mini-Lerneinheit: Wenn `preis` und `menge` Zahlen enthalten, ergibt `preis * menge` den Gesamtpreis.'}
    # math numeric evaluation for common generated tasks
    if task['task_type']=='math':
        nums={'5','10','14','20','60','68','11'}
        if a.replace(',','.') in nums:
            return {'correct':True,'score':100,'feedback':'✅ Das Ergebnis stimmt.','error_type':'none','next_hint':'','remediation':''}
        return {'correct':False,'score':20,'feedback':'❌ Das Ergebnis stimmt noch nicht.','error_type':'calculation','next_hint':'Rechne den Ausdruck Schritt für Schritt und prüfe zuerst die Klammern bzw. die Grundoperation.','remediation':'Mini-Lerneinheit: Schreibe jeden Rechenschritt einzeln auf. So lässt sich erkennen, an welcher Stelle sich der Fehler einschleicht.'}
    # generic text fallback
    ok=any(k in a for k in ['beispiel','erklärung','dictionary','klasse','änderung','beweis','hälfte','bewegung','quelle','filtr','neue stoffe','simple present','i play','dialog'])
    return {'correct':ok,'score':80 if ok else 30,'feedback':'✅ Deine Antwort enthält einen passenden Kern.' if ok else '💡 Die Antwort braucht noch einen klareren Bezug zur gefragten Kompetenz.','error_type':'none' if ok else 'concept','next_hint':'Nenne zuerst den zentralen Begriff und erkläre ihn anschließend mit einem eigenen Beispiel.','remediation':'Mini-Lerneinheit: Erkläre die Kompetenz in einem Satz und ergänze danach ein konkretes Beispiel.'}

def build_remediation(error_type, task, answer, comp):
    topic=comp['title'] if comp else 'dieses Thema'
    lessons={
      'concept':f'🧠 **Kurz zurück:** Bei „{topic}“ fehlt noch ein Teil des Grundkonzepts. Erkläre zuerst den Begriff mit deinen eigenen Worten. Danach wenden wir ihn an.',
      'calculation':'🧮 **Mini-Lerneinheit:** Schreibe den Rechenweg Schritt für Schritt auf. Erst die erste Operation, dann die nächste. So finden wir den genauen Fehler.',
      'syntax':'💻 **Mini-Lerneinheit:** Prüfe zuerst die Struktur des Codes: Variablennamen, `=`, Klammern und `print()`. Ein kleiner Syntaxfehler kann den ganzen Code stoppen.',
      'incomplete':'✏️ **Mini-Lerneinheit:** Deine Idee ist ein Anfang. Zerlege die Aufgabe in kleinere Schritte und beantworte zuerst nur den ersten Schritt.',
      'unclear':'🔎 **Mini-Lerneinheit:** Deine Antwort ist noch zu unklar. Verwende einen konkreten Begriff und ein Beispiel, damit wir dein Verständnis prüfen können.'
    }
    return lessons.get(error_type, f'📚 Wir festigen „{topic}“ kurz und versuchen die Aufgabe danach erneut.')

def update_error_profile(conn,user_id,position,error_type,score):
    if not error_type or error_type == 'none':
        return
    conn.execute('''INSERT INTO error_profiles(user_id,competency_position,error_type,occurrences,last_score)
        VALUES(?,?,?,?,?) ON CONFLICT(user_id,competency_position,error_type) DO UPDATE SET
        occurrences=error_profiles.occurrences+1,last_score=excluded.last_score,last_seen=CURRENT_TIMESTAMP''',
        (user_id,position,error_type,1,score))

def error_profile_for(conn,user_id,position):
    rows=conn.execute('''SELECT error_type,occurrences,last_score,last_seen FROM error_profiles
        WHERE user_id=? AND competency_position=? ORDER BY occurrences DESC,last_seen DESC''',(user_id,position)).fetchall()
    return [dict(r) for r in rows]

def learning_diagnosis(conn,user_id,position):
    rows=error_profile_for(conn,user_id,position)
    if not rows:
        return {'primary_error':None,'patterns':[],'message':'Noch kein wiederkehrendes Fehlermuster erkannt.'}
    primary=rows[0]
    labels={'concept':'Konzeptverständnis','calculation':'Rechenfehler','syntax':'Syntax/Schreibweise','incomplete':'Unvollständige Lösung','unclear':'Unklare Erklärung'}
    msg=f"Wiederkehrendes Muster: {labels.get(primary['error_type'],primary['error_type'])} ({primary['occurrences']}×)."
    return {'primary_error':primary['error_type'],'patterns':rows,'message':msg}

def recent_attempt_context(conn,user_id,comp_position):
    rows=conn.execute("""SELECT t.title,t.content,a.answer,a.correct,a.feedback,a.created_at
        FROM task_attempts a JOIN learning_tasks t ON t.id=a.task_id
        WHERE a.user_id=? AND (t.generated_competency_position=? OR t.generated_competency_position IS NULL)
        ORDER BY a.id DESC LIMIT 5""",(user_id,comp_position)).fetchall()
    return [dict(r) for r in reversed(rows)]


def create_llm_task(conn,user_id,comp,plan):
    user=conn.execute("SELECT name,goal,level,skills FROM users WHERE id=?",(user_id,)).fetchone()
    ctx={'attempts':recent_attempt_context(conn,user_id,comp["position"]),'error_profile':error_profile_for(conn,user_id,comp["position"])}
    obj=llm_generate_task(user,comp,plan,ctx) if user else None
    if not obj: return None
    path=plan['subject'] + (' – Fortgeschritten' if is_advanced_target(plan['target_level']) else ' für Anfänger')
    cur=conn.execute("""INSERT INTO learning_tasks(path,module,title,content,task_type,answer,xp,position,generated_for_user,generated_competency_position,generated)
        VALUES(?,?,?,?,?,?,?,?,?,?,1)""",(path,'🤖 LLM-adaptiv',obj['title'],obj['content'],obj['task_type'],obj['answer'],obj['xp'],910000+int(comp['position'])*100+int(comp['mastery']),user_id,int(comp['position'])))
    # Store the tutor's evaluation rubric separately so it never has to be shown to the learner.
    try: conn.execute("ALTER TABLE learning_tasks ADD COLUMN feedback_hint TEXT NOT NULL DEFAULT ''")
    except sqlite3.OperationalError: pass
    conn.execute("UPDATE learning_tasks SET feedback_hint=? WHERE id=?",(obj.get('feedback_hint',''),cur.lastrowid))
    return conn.execute('SELECT * FROM learning_tasks WHERE id=?',(cur.lastrowid,)).fetchone()


def _generated_task_for(conn, user_id, comp, plan):
    """Create/reuse a personalized transfer task for the current competence gap."""
    existing=conn.execute("SELECT * FROM learning_tasks WHERE generated=1 AND generated_for_user=? AND generated_competency_position=? ORDER BY id DESC LIMIT 1",(user_id,comp['position'])).fetchone()
    if existing:
        attempts=conn.execute('SELECT COUNT(*) FROM task_attempts WHERE user_id=? AND task_id=?',(user_id,existing['id'])).fetchone()[0]
        last=conn.execute('SELECT correct FROM task_attempts WHERE user_id=? AND task_id=? ORDER BY id DESC LIMIT 1',(user_id,existing['id'])).fetchone()
        if attempts == 0 or (last and not last['correct']): return existing
    llm_task=create_llm_task(conn,user_id,comp,plan)
    if llm_task:
        return llm_task
    path=plan['subject'] + (' – Fortgeschritten' if is_advanced_target(plan['target_level']) else ' für Anfänger')    topic=(comp['topic'] or comp['title']).lower(); mastery=int(comp['mastery']); variant='Grundlagen' if mastery < 35 else ('Transfer' if mastery < 65 else 'Challenge')
    title=f"KI-Aufgabe · {comp['title']} · {variant}"; content=''; answer=''; task_type='text'; xp=25
    subject=plan['subject']
    if subject == 'Python':
        if 'variab' in topic or 'grundlag' in topic:
            if mastery < 35:
                content='Schreibe ein kurzes Python-Programm: Speichere die Zahl 7 in einer Variable `zahl` und gib die Variable mit `print()` aus.'; answer='zahl = 7\nprint(zahl)'; task_type='code'; xp=20
            else:
                content='Schreibe Python-Code, der `preis = 12` und `menge = 3` speichert und anschließend den Gesamtpreis ausgibt.'; answer='preis = 12\nmenge = 3\nprint(preis * menge)'; task_type='code'; xp=30
        elif 'logik' in topic or 'beding' in topic:
            content='Schreibe eine if-Bedingung, die `alter = 20` prüft und „Volljährig“ ausgibt, wenn das Alter mindestens 18 ist.'; answer='if alter >= 18'; task_type='code'; xp=30
        else:
            content='Schreibe eine kleine Funktion `quadriere(x)`, die das Quadrat von x zurückgibt. Zeige außerdem einen Aufruf mit 5.'; answer='def quadriere'; task_type='code'; xp=40
    elif subject == 'Mathematik':
        if 'gleich' in topic or 'algebra' in topic:
            content='Löse die Gleichung 3x + 6 = 21. Schreibe auch einen kurzen Rechenweg dazu.'; answer='5'; task_type='math'; xp=30
        elif 'prozent' in topic:
            content='Ein Pullover kostet 80 €. Er wird um 15 % reduziert. Wie viel kostet er danach?'; answer='68'; task_type='math'; xp=30
        else:
            content='Berechne: 4 · (3 + 2) − 6. Erkläre kurz, welche Rechenregel du zuerst benutzt.'; answer='14'; task_type='math'; xp=25
    elif subject == 'Englisch':
        content='Schreibe drei englische Sätze über deinen Alltag. Verwende mindestens einmal „I“, einmal ein Verb im Simple Present und einmal eine Zeitangabe.'; answer='simple present'; task_type='text'; xp=30
    elif subject == 'Physik':
        content='Ein Fahrrad fährt 30 km in 1,5 Stunden. Berechne die Durchschnittsgeschwindigkeit und schreibe die verwendete Formel dazu.'; answer='20'; task_type='math'; xp=30
    elif subject == 'Chemie':
        content='Erkläre in 2–3 Sätzen, was bei einer chemischen Reaktion grundsätzlich mit den Ausgangsstoffen passiert.'; answer='neue stoffe'; task_type='text'; xp=30
    else:
        content=f'Erkläre die Kompetenz „{comp["title"]}“ mit einem eigenen Beispiel aus der Praxis.'; answer='beispiel'; task_type='text'; xp=25
    cur=conn.execute("""INSERT INTO learning_tasks(path,module,title,content,task_type,answer,xp,position,generated_for_user,generated_competency_position,generated)
        VALUES(?,?,?,?,?,?,?,?,?,?,1)""",(path,'🤖 KI-generiert',title,content,task_type,answer,xp,900000+int(comp['position'])*100+int(comp['mastery']),user_id,int(comp['position'])))
    return conn.execute('SELECT * FROM learning_tasks WHERE id=?',(cur.lastrowid,)).fetchone()

def task_competency_position(task, tasks, step_count):
    explicit=task['generated_competency_position'] if 'generated_competency_position' in task.keys() else None
    return int(explicit) if explicit is not None else competency_index_for_task(task,tasks,step_count)

def adaptive_recommendation(conn, user_id, plan):
    if not plan: return None
    steps=json.loads(plan['plan_json']); sync_plan_competencies(conn,user_id,steps)
    competencies=conn.execute('SELECT * FROM learning_competencies WHERE user_id=? ORDER BY position',(user_id,)).fetchall()
    if not competencies: return None
    comp=next((c for c in competencies if c['status']!='mastered'),None)
    if not comp: return {'status':'completed','message':'Alle Kompetenzen dieses Lernpfads wurden beherrscht.','competency':None,'task':None}
    task_path=plan['subject'] + (' – Fortgeschritten' if is_advanced_target(plan['target_level']) else ' für Anfänger')
    task_rows=conn.execute('SELECT * FROM learning_tasks WHERE path=? AND (generated=0 OR generated_for_user=?) ORDER BY position',(task_path,user_id)).fetchall()
    mapped=[t for t in task_rows if task_competency_position(t,task_rows,len(steps))==comp['position']]
    attempts=conn.execute('SELECT task_id, COUNT(*) attempts, MAX(correct) correct FROM task_attempts WHERE user_id=? GROUP BY task_id',(user_id,)).fetchall(); amap={a['task_id']:dict(a) for a in attempts}
    incomplete=[t for t in mapped if t['id'] not in amap or not amap[t['id']]['correct']]
    if comp['mastery'] < 35 and incomplete:
        candidate=incomplete[0]; reason='Deine Kompetenz ist noch am Anfang – wir festigen zuerst die Grundlagen.'
    elif comp['mastery'] < 80:
        candidate=_generated_task_for(conn,user_id,comp,plan); reason='Du hast die Grundlage bereits gezeigt. Die KI gibt dir jetzt eine neue Aufgabe, damit du das Wissen selbstständig übertragen kannst.'
    elif incomplete:
        candidate=incomplete[-1]; reason='Die Kompetenz ist fast beherrscht – zeige sie noch einmal, damit sie als sicher gilt.'
    else:
        candidate=_generated_task_for(conn,user_id,comp,plan); reason='Die Grundlagen sitzen. Zeige die Kompetenz an einer neuen Aufgabe, bevor wir weitergehen.'
    return {'status':'ready','message':reason,'reason':reason,'competency':dict(comp),'task':dict(candidate),'task_attempts':amap.get(candidate['id'],{}).get('attempts',0),'mastery':comp['mastery'],'target_mastery':80,'generated':bool(candidate['generated']),'diagnosis':learning_diagnosis(conn,user_id,comp['position'])}

def evaluate_task(task, answer):
    a=answer.strip().lower()
    if not a: return False,'Bitte gib zuerst deine eigene Lösung ein.'
    if task['generated']:
        content=task['content'].lower()
        if 'zahl = 7' in content:
            ok=('zahl' in a and '=' in a and '7' in a and 'print' in a)
            return (True,'✅ Sehr gut! Du hast Variable und Ausgabe korrekt verbunden.') if ok else (False,'💡 Lege zuerst `zahl = 7` an und gib danach `zahl` mit `print()` aus.')
        if 'gesamtpreis' in content:
            ok=('preis' in a and 'menge' in a and 'print' in a and ('*' in a or 'preis * menge' in a))
            return (True,'✅ Genau! Du hast Variablen sinnvoll kombiniert.') if ok else (False,'💡 Speichere Preis und Menge getrennt und multipliziere beide Werte.')
        if 'if-bedingung' in content or 'if-bedingung' in content or 'mindestens 18' in content:
            ok=('if' in a and ('alter' in a or '>=' in a) and ('18' in a or 'volljährig' in a))
            return (True,'✅ Die Bedingung passt.') if ok else (False,'💡 Du brauchst `if`, eine Altersprüfung wie `alter >= 18` und eine Ausgabe.')
        if 'quadriere' in content:
            ok=('def' in a and 'quadriere' in a)
            return (True,'✅ Gute Funktion. Du hast das Konzept auf eine neue Aufgabe übertragen.') if ok else (False,'💡 Beginne mit `def quadriere(x):` und gib `x * x` zurück.')
        if '3x + 6 = 21' in task['content']:
            ok=bool(re.search(r'(?<!\d)5(?!\d)',a))
            return (True,'✅ Richtig! x = 5.') if ok else (False,'💡 Ziehe zuerst 6 ab und teile anschließend durch 3.')
        if '15 % reduziert' in task['content']:
            ok=bool(re.search(r'(?<!\d)68(?!\d)',a))
            return (True,'✅ Richtig! Der neue Preis beträgt 68 €.') if ok else (False,'💡 15 % von 80 € sind 12 €. Ziehe 12 € von 80 € ab.')
        if '4 · (3 + 2)' in task['content']:
            ok=bool(re.search(r'(?<!\d)14(?!\d)',a))
            return (True,'✅ Richtig! Erst die Klammer, dann Multiplikation, dann Subtraktion.') if ok else (False,'💡 Berechne zuerst die Klammer: 3 + 2.')
        if 'drei englische sätze' in content:
            ok=len(a.split())>=8 and ('i ' in a or a.startswith('i '))
            return (True,'✅ Sehr gut. Du verwendest Englisch selbstständig.') if ok else (False,'💡 Schreibe mindestens drei kurze englische Sätze und verwende „I“ sowie eine Zeitangabe.')
        if 'fahrrad fährt 30 km' in content:
            ok=bool(re.search(r'(?<!\d)20(?!\d)',a))
            return (True,'✅ Richtig! 30 km / 1,5 h = 20 km/h.') if ok else (False,'💡 Verwende v = s / t: 30 / 1,5.')
        if 'chemischen reaktion' in content:
            ok=('neu' in a and ('stoff' in a or 'stoffe' in a))
            return (True,'✅ Genau: Bei einer chemischen Reaktion können neue Stoffe mit neuen Eigenschaften entstehen.') if ok else (False,'💡 Denke daran, dass sich Ausgangsstoffe umwandeln und neue Stoffe entstehen können.')
        return (True,'✅ Gute eigene Lösung. Du hast die Kompetenz praktisch angewendet.') if len(a.split())>=4 else (False,'💡 Erkläre deine Lösung noch etwas genauer und gib ein konkretes Beispiel an.')
    path=task['path']; pos=task['position']
    if path=='Python für Anfänger':
        checks={1:('variable' in a or 'wert' in a, 'Erkläre noch kurz, dass eine Variable einen Wert unter einem Namen speichert.'),2:('print' in a and 'hallo' in a, 'Verwende `print()` und gib „Hallo Welt!“ aus.'),3:('name' in a and 'print' in a, 'Lege `name` an und verwende die Variable anschließend in `print()`.'),4:('if' in a and ('alter' in a or '>=' in a), 'Prüfe mit `if`, ob `alter >= 18` gilt.'),5:('for' in a, 'Versuche eine `for`-Schleife zu verwenden.'),6:('print' in a and ('+' in a or 'sum' in a), 'Dein Rechner braucht zwei Werte, eine Addition und eine Ausgabe.')}
    elif path=='Mathematik für Anfänger':
        checks={101:('11' in a, 'Setze x=4 ein: 2·4+3.'),102:('hälfte' in a or 'half' in a or '50' in a, '1/2 beschreibt die Hälfte eines Ganzen.'),103:('3' in a, 'Ziehe 4 ab und teile anschließend durch 2.'),104:('10' in a, '20 % von 50 entspricht 0,2 · 50.'),105:('60' in a, '25 % von 80 sind 20 €. Ziehe diese von 80 € ab.')}
    elif path=='Englisch für Anfänger':
        checks={201:('my name' in a or 'i am' in a, 'Beginne mit „My name is …“ oder „I am …“.'),202:('i play' in a, 'Ein einfacher Satz kann mit „I play …“ beginnen.'),203:(len(a.split())>=8, 'Nenne mindestens fünf englische Wörter und ihre Bedeutungen.'),204:('do you' in a, 'Eine einfache Frage kann mit „Do you …?“ beginnen.'),205:(len(a.split())>=12, 'Schreibe mindestens vier kurze englische Sätze.')}
    elif path=='Physik für Anfänger':
        checks={301:('beweg' in a or 'kraft' in a, 'Denke daran: Kräfte können Bewegung ändern oder Körper verformen.'),302:('50' in a, 'Geschwindigkeit = Strecke / Zeit.'),303:(len(a.split(','))>=3 or len(a.split())>=6, 'Nenne drei Energieformen, z. B. Bewegungs-, Lage- oder elektrische Energie.'),304:('quelle' in a and ('leiter' in a or 'verbrauch' in a or 'gerät' in a), 'Denke an Energiequelle, leitenden Weg und Verbraucher.'),305:(len(a)>30, 'Erkläre Ursache und Wirkung eines konkreten Alltagsphänomens.')}
    elif path=='Python – Fortgeschritten':
        checks={1001:('def' in a and 'mittelwert' in a, 'Beginne mit einer Funktion `def mittelwert(zahlen):`.'),1002:('dictionary' in a or 'schlüssel' in a or 'key' in a, 'Ein Dictionary ist besonders nützlich, wenn Werte über Schlüssel gefunden werden sollen.'),1003:('klasse' in a or 'objekt' in a, 'Denke an eigene Objekte mit gemeinsamem Aufbau und Verhalten.')}
    elif path=='Mathematik – Fortgeschritten':
        checks={1101:('änder' in a or 'steigung' in a or 'rate' in a, 'Die Ableitung beschreibt vereinfacht die momentane Änderungsrate bzw. Steigung.'),1102:('11' in a, 'Rechne 2·4 + 3·1.'),1103:('beweis' in a or 'logisch' in a or 'begründ' in a, 'Ein Beweis begründet eine Aussage nachvollziehbar aus Definitionen, Annahmen und bereits gesicherten Aussagen.')}
    else:
        checks={401:('atom' in a and len(a)>15, 'Ein Atom ist ein sehr kleiner Baustein der Materie.'),402:('atomsorte' in a or 'atomen' in a or 'gleiche' in a, 'Ein Element besteht aus Atomen mit derselben Protonenzahl.'),403:('reaktion' in a or 'rost' in a or 'verbren' in a, 'Nenne einen Vorgang, bei dem neue Stoffe entstehen.'),404:('filtr' in a or 'destill' in a or 'sieb' in a, 'Nenne z. B. Filtration, Destillation oder Sieben.'),405:(len(a)>30, 'Beschreibe einen konkreten chemischen Vorgang aus deinem Alltag.')}
    ok, hint=checks.get(pos,(False,'Ich brauche noch mehr Informationen zu dieser Aufgabe.'))
    if ok:
        return True,'✅ Sehr gut! Du hast den Kern der Aufgabe verstanden.'
    return False,'💡 Noch nicht ganz. '+hint


def get_curriculum(conn, country, school_type, year_level, subject):
    return conn.execute('SELECT * FROM curricula WHERE country=? AND school_type=? AND year_level=? AND subject=?',(country,school_type,year_level,subject)).fetchone()

def curriculum_context(conn, user_id):
    uc=conn.execute('SELECT * FROM user_curriculum WHERE user_id=?',(user_id,)).fetchone()
    if not uc: return None, []
    cur=conn.execute('SELECT * FROM curricula WHERE id=?',(uc['curriculum_id'],)).fetchone() if uc['curriculum_id'] else None
    comps=conn.execute('''SELECT c.*,COALESCE(ucm.status,'not_started') status,COALESCE(ucm.mastery,0) mastery
        FROM competencies c LEFT JOIN user_competencies ucm ON ucm.competency_id=c.id AND ucm.user_id=?
        WHERE c.curriculum_id=? ORDER BY c.position''',(user_id,uc['curriculum_id'])).fetchall() if cur else []
    return cur,[dict(x) for x in comps]

def github_enabled():
    return bool(GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET)

def github_api(path, token):
    req=urllib.request.Request('https://api.github.com'+path, headers={
        'Accept':'application/vnd.github+json','Authorization':'Bearer '+token,
        'X-GitHub-Api-Version':GITHUB_API_VERSION,'User-Agent':'NextStep/1.0'})
    with urllib.request.urlopen(req, timeout=12) as resp:
        return json.loads(resp.read().decode('utf-8'))

def github_token_exchange(code):
    data=urlencode({'client_id':GITHUB_CLIENT_ID,'client_secret':GITHUB_CLIENT_SECRET,'code':code,'redirect_uri':GITHUB_REDIRECT_URI}).encode()
    req=urllib.request.Request('https://github.com/login/oauth/access_token',data=data,method='POST',headers={'Accept':'application/json','User-Agent':'NextStep/1.0'})
    with urllib.request.urlopen(req, timeout=12) as resp:
        return json.loads(resp.read().decode('utf-8'))

def github_connection_payload(conn,user_id):
    row=conn.execute('SELECT * FROM github_connections WHERE user_id=?',(user_id,)).fetchone()
    if not row: return {'connected':False,'enabled':github_enabled()}
    return {'connected':True,'enabled':github_enabled(),'login':row['login'],'name':row['name'],'avatar_url':row['avatar_url'],'html_url':row['html_url'],'public_repos':row['public_repos'],'languages':json.loads(row['languages_json'] or '[]'),'repos':json.loads(row['repos_json'] or '[]'),'updated_at':row['updated_at']}

def sync_github(conn,user_id,token):
    me=github_api('/user',token)
    repos=github_api('/user/repos?visibility=public&affiliation=owner,collaborator,organization_member&sort=updated&per_page=50',token)
    if not isinstance(repos,list): repos=[]
    language_counts={}; repo_out=[]
    for r in repos:
        if not r.get('fork',False) and r.get('language'):
            lang=r['language']; language_counts[lang]=language_counts.get(lang,0)+1
        repo_out.append({'name':r.get('name',''),'html_url':r.get('html_url',''),'description':r.get('description') or '','language':r.get('language') or '', 'topics':r.get('topics') or []})
    languages=sorted(language_counts.items(),key=lambda x:(-x[1],x[0].lower()))
    conn.execute("""INSERT INTO github_connections(user_id,github_user_id,login,name,avatar_url,html_url,access_token,refresh_token,expires_at,scope,public_repos,languages_json,repos_json,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET github_user_id=excluded.github_user_id,login=excluded.login,name=excluded.name,avatar_url=excluded.avatar_url,html_url=excluded.html_url,access_token=excluded.access_token,refresh_token=excluded.refresh_token,expires_at=excluded.expires_at,scope=excluded.scope,public_repos=excluded.public_repos,languages_json=excluded.languages_json,repos_json=excluded.repos_json,updated_at=CURRENT_TIMESTAMP""",
        (user_id,me.get('id'),me.get('login',''),me.get('name') or '',me.get('avatar_url') or '',me.get('html_url') or '',token,'',None,'read:user',me.get('public_repos',0),json.dumps(languages,ensure_ascii=False),json.dumps(repo_out[:20],ensure_ascii=False)))
    return github_connection_payload(conn,user_id)

def reddit_enabled():
    return bool(REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET)

def reddit_token_exchange(code):
    raw=('%s:%s' % (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)).encode()
    auth=base64.b64encode(raw).decode()
    data=urlencode({'grant_type':'authorization_code','code':code,'redirect_uri':REDDIT_REDIRECT_URI}).encode()
    req=urllib.request.Request('https://www.reddit.com/api/v1/access_token',data=data,method='POST',headers={
        'Authorization':'Basic '+auth,'User-Agent':REDDIT_USER_AGENT,'Content-Type':'application/x-www-form-urlencoded'})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode('utf-8'))

def reddit_api(path, token):
    req=urllib.request.Request('https://oauth.reddit.com'+path,headers={
        'Authorization':'Bearer '+token,'User-Agent':REDDIT_USER_AGENT,'Accept':'application/json'})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode('utf-8'))

def reddit_connection_payload(conn,user_id):
    row=conn.execute('SELECT * FROM reddit_connections WHERE user_id=?',(user_id,)).fetchone()
    if not row: return {'connected':False,'enabled':reddit_enabled()}
    communities=json.loads(row['communities_json'] or '[]')
    return {'connected':True,'enabled':reddit_enabled(),'username':row['username'],'icon_img':row['icon_img'],
            'communities':communities,'use_for_matching':bool(row['use_for_matching']),
            'selected_communities':[x['name'] for x in communities if x.get('selected')],
            'updated_at':row['updated_at']}

def sync_reddit(conn,user_id,token,refresh_token=''):
    me=reddit_api('/api/v1/me',token)
    username=me.get('name') or ''
    if not username: raise ValueError('Reddit hat keinen Benutzernamen geliefert.')
    communities={}
    for endpoint in [f'/user/{quote(username)}/submitted?limit=50&raw_json=1',f'/user/{quote(username)}/comments?limit=50&raw_json=1']:
        try:
            data=reddit_api(endpoint,token)
            children=((data.get('data') or {}).get('children') or []) if isinstance(data,dict) else []
            for item in children:
                d=item.get('data') or {}
                sr=d.get('subreddit')
                if sr: communities[sr]=communities.get(sr,0)+1
        except Exception:
            continue
    old_row=conn.execute('SELECT communities_json,use_for_matching FROM reddit_connections WHERE user_id=?',(user_id,)).fetchone()
    old_selected=set()
    if old_row:
        try: old_selected={x.get('name') for x in json.loads(old_row['communities_json'] or '[]') if x.get('selected')}
        except Exception: old_selected=set()
    community_list=sorted([{'name':k,'activity':v,'selected':k in old_selected} for k,v in communities.items()],key=lambda x:(-x['activity'],x['name'].lower()))[:30]
    conn.execute("""INSERT INTO reddit_connections(user_id,reddit_user_id,username,icon_img,access_token,refresh_token,expires_at,scope,communities_json,use_for_matching,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET reddit_user_id=excluded.reddit_user_id,username=excluded.username,icon_img=excluded.icon_img,access_token=excluded.access_token,refresh_token=excluded.refresh_token,expires_at=excluded.expires_at,scope=excluded.scope,communities_json=excluded.communities_json,use_for_matching=excluded.use_for_matching,updated_at=CURRENT_TIMESTAMP""",
        (user_id,str(me.get('id') or ''),username,me.get('icon_img') or '',token,refresh_token,None,'identity read',json.dumps(community_list,ensure_ascii=False),int(bool(old_row and old_row['use_for_matching']))))
    return reddit_connection_payload(conn,user_id)

def reddit_selected_communities(conn,user_id):
    row=conn.execute('SELECT communities_json,use_for_matching FROM reddit_connections WHERE user_id=?',(user_id,)).fetchone()
    if not row or not row['use_for_matching']: return set()
    try: return {str(x.get('name')).lower() for x in json.loads(row['communities_json'] or '[]') if x.get('selected')}
    except Exception: return set()

class Handler(BaseHTTPRequestHandler):
    def send_json(self, status, data, cookies=None):
        payload = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Cache-Control', 'no-store')
        if cookies:
            for c in cookies: self.send_header('Set-Cookie', c)
        self.end_headers(); self.wfile.write(payload)

    def do_GET(self):
        parsed=urlparse(self.path); path=parsed.path
        if path == '/api/integrations/github/start':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            if not github_enabled(): return self.send_json(503, {'error':'GitHub ist noch nicht konfiguriert. Hinterlege NEXTSTEP_GITHUB_CLIENT_ID und NEXTSTEP_GITHUB_CLIENT_SECRET.'})
            state=secrets.token_urlsafe(32); SESSIONS['github_state:'+state]=row['id']
            params={'client_id':GITHUB_CLIENT_ID,'redirect_uri':GITHUB_REDIRECT_URI,'scope':'read:user','state':state}
            self.send_response(302); self.send_header('Location','https://github.com/login/oauth/authorize?'+urlencode(params)); self.end_headers(); return
        if path == '/api/integrations/github/callback':
            q=parse_qs(parsed.query); state=(q.get('state') or [''])[0]; code=(q.get('code') or [''])[0]
            uid=SESSIONS.pop('github_state:'+state,None) if state else None
            if not uid or not code:
                self.send_response(302); self.send_header('Location','/?github=error'); self.end_headers(); return
            try:
                token_data=github_token_exchange(code); token=token_data.get('access_token')
                if not token: raise ValueError('GitHub hat kein Zugriffstoken geliefert.')
                conn=db(); sync_github(conn,uid,token); conn.commit(); conn.close()
                self.send_response(302); self.send_header('Location','/?github=connected'); self.end_headers(); return
            except Exception:
                self.send_response(302); self.send_header('Location','/?github=error'); self.end_headers(); return
        if path == '/api/integrations/github':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); payload=github_connection_payload(conn,row['id']); conn.close(); return self.send_json(200,payload)
        if path == '/api/integrations/reddit/start':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            if not reddit_enabled(): return self.send_json(503, {'error':'Reddit ist noch nicht konfiguriert. Hinterlege NEXTSTEP_REDDIT_CLIENT_ID und NEXTSTEP_REDDIT_CLIENT_SECRET.'})
            state=secrets.token_urlsafe(32); SESSIONS['reddit_state:'+state]=row['id']
            params={'client_id':REDDIT_CLIENT_ID,'response_type':'code','state':state,'redirect_uri':REDDIT_REDIRECT_URI,'duration':'permanent','scope':'identity read'}
            self.send_response(302); self.send_header('Location','https://www.reddit.com/api/v1/authorize?'+urlencode(params)); self.end_headers(); return
        if path == '/api/integrations/reddit/callback':
            q=parse_qs(parsed.query); state=(q.get('state') or [''])[0]; code=(q.get('code') or [''])[0]
            uid=SESSIONS.pop('reddit_state:'+state,None) if state else None
            if not uid or not code:
                self.send_response(302); self.send_header('Location','/?reddit=error'); self.end_headers(); return
            try:
                token_data=reddit_token_exchange(code); token=token_data.get('access_token')
                if not token: raise ValueError('Reddit hat kein Zugriffstoken geliefert.')
                conn=db(); sync_reddit(conn,uid,token,token_data.get('refresh_token','')); conn.commit(); conn.close()
                self.send_response(302); self.send_header('Location','/?reddit=connected'); self.end_headers(); return
            except Exception:
                self.send_response(302); self.send_header('Location','/?reddit=error'); self.end_headers(); return
        if path == '/api/integrations/reddit':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); payload=reddit_connection_payload(conn,row['id']); conn.close(); return self.send_json(200,payload)
        if path == '/api/integrations/reddit/sync':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); r=conn.execute('SELECT access_token,refresh_token FROM reddit_connections WHERE user_id=?',(row['id'],)).fetchone()
            if not r: conn.close(); return self.send_json(404, {'error':'Reddit ist nicht verbunden.'})
            try:
                payload=sync_reddit(conn,row['id'],r['access_token'],r['refresh_token']); conn.commit(); conn.close(); return self.send_json(200,payload)
            except Exception:
                conn.close(); return self.send_json(502, {'error':'Reddit konnte nicht synchronisiert werden.'})
        if path == '/api/integrations/reddit/disconnect':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); conn.execute('DELETE FROM reddit_connections WHERE user_id=?',(row['id'],)); conn.commit(); conn.close(); return self.send_json(200, {'connected':False,'message':'Reddit-Verbindung entfernt.'})
        if path == '/api/health':
            try:
                conn=db()
                conn.execute('SELECT 1').fetchone()
                conn.close()
                self.send_json(200, {
                    'ok': True,
                    'database': 'postgresql' if DATABASE_URL else 'sqlite',
                    'persistent': bool(DATABASE_URL),
                    'message': 'NextStep-Datenbank ist verbunden.'
                })
            except Exception as exc:
                print('DATABASE HEALTH ERROR:', exc)
                self.send_json(503, {
                    'ok': False,
                    'database': 'postgresql' if DATABASE_URL else 'sqlite',
                    'persistent': bool(DATABASE_URL),
                    'message': 'NextStep-Datenbank ist nicht erreichbar.'
                })
            return

        if path == '/api/me':
            self.send_json(200, {'user': public_user(session_user(self))}); return
        if path == '/api/integrations/github/sync':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); g=conn.execute('SELECT access_token FROM github_connections WHERE user_id=?',(row['id'],)).fetchone()
            if not g: conn.close(); return self.send_json(404, {'error':'GitHub ist nicht verbunden.'})
            try:
                payload=sync_github(conn,row['id'],g['access_token']); conn.commit(); conn.close(); return self.send_json(200,payload)
            except Exception:
                conn.close(); return self.send_json(502, {'error':'GitHub konnte nicht synchronisiert werden.'})
        if path == '/api/integrations/github/disconnect':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); conn.execute('DELETE FROM github_connections WHERE user_id=?',(row['id'],)); conn.commit(); conn.close(); return self.send_json(200, {'connected':False,'message':'GitHub-Verbindung entfernt.'})
        if path == '/api/integrations/reddit/matching':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            selected=body.get('selected_communities',[]) if isinstance(body,dict) else []
            selected={str(x).strip().lstrip('r/').lower() for x in selected if str(x).strip()}
            enabled=bool(body.get('enabled')) if isinstance(body,dict) else False
            conn=db(); current=conn.execute('SELECT communities_json FROM reddit_connections WHERE user_id=?',(row['id'],)).fetchone()
            if not current: conn.close(); return self.send_json(404, {'error':'Reddit ist nicht verbunden.'})
            communities=json.loads(current['communities_json'] or '[]')
            for item in communities: item['selected']=item.get('name','').lower() in selected
            conn.execute('UPDATE reddit_connections SET communities_json=?,use_for_matching=?,updated_at=CURRENT_TIMESTAMP WHERE user_id=?',(json.dumps(communities,ensure_ascii=False),int(enabled),row['id']))
            conn.commit(); payload=reddit_connection_payload(conn,row['id']); conn.close(); return self.send_json(200,payload)

        if path == '/api/profile':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); profile=profile_payload(conn,row['id']); matches=matching_users(conn,row['id']); conn.close()
            return self.send_json(200, {'profile':profile,'matches':matches})
        if path == '/api/dashboard':
            row = session_user(self)
            if not row:
                self.send_json(401, {'error':'Nicht eingeloggt.'}); return
            conn = db()
            higher = conn.execute('SELECT COUNT(*) FROM users WHERE xp > ?', (row['xp'],)).fetchone()[0]
            total = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
            conn.close()
            rank = higher + 1
            xp = row['xp']
            level = xp // 100 + 1
            next_level_xp = level * 100
            current_level_xp = (level - 1) * 100
            progress = round((xp - current_level_xp) / 100 * 100)
            self.send_json(200, {'user': public_user(row), 'xp': xp, 'rank': rank, 'total_users': total, 'level': level, 'next_level_xp': next_level_xp, 'progress': progress}); return
        if path == '/api/leaderboard':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db()
            users=conn.execute('SELECT id,name,xp,created_at FROM users ORDER BY xp DESC, id ASC LIMIT 100').fetchall()
            total=conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
            conn.close()
            leaderboard=[]
            for idx,u in enumerate(users,1):
                leaderboard.append({'rank':idx,'id':u['id'],'name':u['name'],'xp':u['xp'],'is_current_user':u['id']==row['id']})
            self.send_json(200, {'leaderboard':leaderboard,'total_users':total})
            return
        if path == '/api/community':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db()
            mode = (urlparse(self.path).query or '').replace('mode=','') or 'for-you'
            if mode == 'following':
                where = 'p.user_id IN (SELECT following_id FROM user_follows WHERE follower_id=?)'
                params=(row['id'], row['id'], row['id'])
                order='p.id DESC'
            elif mode == 'trending':
                where = '1=1'
                params=(row['id'], row['id'])
                order='(p.likes * 3 + (SELECT COUNT(*) FROM community_comments cc WHERE cc.post_id=p.id) * 2) DESC, p.id DESC'
            else:
                where = '1=1'
                params=(row['id'], row['id'])
                order='p.id DESC'
            posts=conn.execute(f'''SELECT p.*,u.name,u.level,u.skills,
                EXISTS(SELECT 1 FROM community_post_likes l WHERE l.post_id=p.id AND l.user_id=?) liked,
                EXISTS(SELECT 1 FROM user_follows f WHERE f.follower_id=? AND f.following_id=p.user_id) following_author
                FROM community_posts p JOIN users u ON u.id=p.user_id
                WHERE {where}
                ORDER BY {order} LIMIT 50''',params).fetchall()
            out=[]
            for p in posts:
                comments=conn.execute('''SELECT c.*,u.name,u.level,u.skills,
                    EXISTS(SELECT 1 FROM community_comment_likes l WHERE l.comment_id=c.id AND l.user_id=?) liked
                    FROM community_comments c JOIN users u ON u.id=c.user_id
                    WHERE c.post_id=? ORDER BY c.id ASC''',(row['id'],p['id'])).fetchall()
                d=dict(p); d['comments']=[dict(c) for c in comments]; out.append(d)
            topics=[r['topic'] for r in conn.execute('SELECT DISTINCT topic FROM community_posts ORDER BY topic').fetchall()]
            following=conn.execute('''SELECT u.id,u.name,u.level,u.skills,
                EXISTS(SELECT 1 FROM user_follows f2 WHERE f2.follower_id=? AND f2.following_id=u.id) following
                FROM users u WHERE u.id<>? ORDER BY u.xp DESC, u.id ASC LIMIT 12''',(row['id'],row['id'])).fetchall()
            counts=conn.execute('SELECT COUNT(*) followers FROM user_follows WHERE following_id=?',(row['id'],)).fetchone()
            conn.close()
            self.send_json(200, {'posts':out,'topics':topics,'current_user_id':row['id'],'mode':mode,'suggested_users':[dict(x) for x in following],'follower_count':counts['followers']}); return

        if path == '/api/tutor/context':
            row = session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db()
            profile=conn.execute('SELECT * FROM tutor_profiles WHERE user_id=?',(row['id'],)).fetchone()
            if not profile:
                conn.execute('INSERT INTO tutor_profiles(user_id,subject,level) VALUES(?,?,?)',(row['id'],'Python',row['level'] or 'Anfänger')); conn.commit()
                profile=conn.execute('SELECT * FROM tutor_profiles WHERE user_id=?',(row['id'],)).fetchone()
            recent=conn.execute("SELECT role,message,created_at FROM tutor_messages WHERE user_id=? ORDER BY id DESC LIMIT 12",(row['id'],)).fetchall()
            uc=conn.execute('SELECT * FROM user_curriculum WHERE user_id=?',(row['id'],)).fetchone()
            if not uc:
                default_cur=get_curriculum(conn,'Österreich','Mittelschule','1. Klasse','Mathematik')
                conn.execute('INSERT INTO user_curriculum(user_id,country,school_type,year_level,subject,curriculum_id) VALUES(?,?,?,?,?,?)',(row['id'],'Österreich','Mittelschule','1. Klasse','Mathematik',default_cur['id']))
                conn.commit()
            cur,comps=curriculum_context(conn,row['id'])
            learning_plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            learning_competencies=[dict(x) for x in conn.execute('SELECT * FROM learning_competencies WHERE user_id=? ORDER BY position',(row['id'],)).fetchall()]
            learning_recommendation=adaptive_recommendation(conn,row['id'],learning_plan) if learning_plan else None
            conn.close()
            self.send_json(200, {'profile':dict(profile),'recent_messages':[dict(x) for x in reversed(recent)],'curriculum':dict(cur) if cur else None,'competencies':comps,'learning_plan':dict(learning_plan) if learning_plan else None,'learning_competencies':learning_competencies,'learning_recommendation':learning_recommendation,'llm_enabled':llm_enabled(),'llm_model':LLM_MODEL})
            return

        if path == '/api/curriculum':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); uc=conn.execute('SELECT * FROM user_curriculum WHERE user_id=?',(row['id'],)).fetchone()
            if not uc:
                subject='Mathematik'; cur=get_curriculum(conn,'Österreich','Mittelschule','1. Klasse',subject)
                conn.execute('INSERT INTO user_curriculum(user_id,curriculum_id,subject) VALUES(?,?,?)',(row['id'],cur['id'],subject)); conn.commit(); uc=conn.execute('SELECT * FROM user_curriculum WHERE user_id=?',(row['id'],)).fetchone()
            cur,comps=curriculum_context(conn,row['id']); conn.close()
            self.send_json(200,{'selection':dict(uc),'curriculum':dict(cur) if cur else None,'competencies':comps}); return

        if path == '/api/learning':
            row = session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db()
            plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            if not plan:
                pref=conn.execute('SELECT * FROM learning_preferences WHERE user_id=?',(row['id'],)).fetchone()
                target=pref['target_level'] if pref else 'Fortgeschrittener Anfänger'
                custom=pref['custom_goal'] if pref and pref['custom_goal'] else row['goal']
                subject,path,steps=build_plan(row,target,custom)
                sync_plan_competencies(conn,row['id'],steps)
                conn.execute('INSERT INTO learning_plans(user_id,goal,subject,current_level,target_level,learning_mode,domain,plan_json) VALUES(?,?,?,?,?,?,?,?)',(row['id'],custom or 'Lernen',subject,row['level'] or 'Anfänger',target,pref['learning_mode'] if pref else 'guided',pref['domain'] if pref else subject,json.dumps(steps,ensure_ascii=False)))
                conn.commit(); plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            task_path=plan['subject'] + (' – Fortgeschritten' if is_advanced_target(plan['target_level']) else ' für Anfänger')
            tasks=conn.execute('SELECT id,path,module,title,content,task_type,xp,position FROM learning_tasks WHERE path=? ORDER BY position',(task_path,)).fetchall()
            done={r['task_id'] for r in conn.execute('SELECT task_id FROM user_task_progress WHERE user_id=?',(row['id'],)).fetchall()}
            attempts=conn.execute('SELECT task_id, COUNT(*) attempts, MAX(correct) correct FROM task_attempts WHERE user_id=? GROUP BY task_id',(row['id'],)).fetchall()
            attempt_map={x['task_id']:dict(x) for x in attempts}
            out=[]
            for t in tasks:
                d=dict(t); d['completed']=t['id'] in done; d['attempts']=attempt_map.get(t['id'],{}).get('attempts',0); d['mastered']=bool(attempt_map.get(t['id'],{}).get('correct',0)); out.append(d)
            steps=json.loads(plan['plan_json'])
            sync_plan_competencies(conn,row['id'],steps)
            competencies=[dict(x) for x in conn.execute('SELECT * FROM learning_competencies WHERE user_id=? ORDER BY position',(row['id'],)).fetchall()]
            next_comp=next((c for c in competencies if c['status']!='mastered'),competencies[0] if competencies else None)
            recommendation=adaptive_recommendation(conn,row['id'],plan)
            conn.commit(); conn.close()
            self.send_json(200, {'path':plan['subject']+' Lernplan','goal':plan['goal'],'current_step':plan['current_step'],'current_level':plan['current_level'],'target_level':plan['target_level'],'steps':steps,'tasks':out,'completed':len(done),'total':len(tasks),'competencies':competencies,'next_competency':next_comp,'recommendation':recommendation})
            return

        if path == '/api/learning/progress':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db()
            # Long-term progress is derived from the existing learning evidence.
            attempts=conn.execute("""SELECT substr(created_at,1,10) day, COUNT(*) attempts,
                COALESCE(SUM(correct),0) correct, COALESCE(AVG(score),0) avg_score
                FROM task_attempts WHERE user_id=? GROUP BY substr(created_at,1,10) ORDER BY day""",(row['id'],)).fetchall()
            xp_days=conn.execute("""SELECT substr(created_at,1,10) day, COALESCE(SUM(xp),0) xp
                FROM xp_events WHERE user_id=? GROUP BY substr(created_at,1,10) ORDER BY day""",(row['id'],)).fetchall()
            comps=conn.execute("""SELECT title,topic,mastery,attempts,correct_attempts,status,updated_at
                FROM learning_competencies WHERE user_id=? ORDER BY position""",(row['id'],)).fetchall()
            all_attempts=conn.execute("""SELECT COUNT(*) n, COALESCE(SUM(correct),0) correct,
                COALESCE(AVG(score),0) score FROM task_attempts WHERE user_id=?""",(row['id'],)).fetchone()
            first=conn.execute("""SELECT COUNT(*) n, COALESCE(SUM(correct),0) correct, COALESCE(AVG(score),0) score
                FROM task_attempts WHERE user_id=? AND id <= (SELECT COALESCE(MIN(id),0)+9 FROM task_attempts WHERE user_id=?)""",(row['id'],row['id'])).fetchone()
            recent=conn.execute("""SELECT COUNT(*) n, COALESCE(SUM(correct),0) correct, COALESCE(AVG(score),0) score
                FROM task_attempts WHERE user_id=? AND id > (SELECT COALESCE(MAX(id),0)-9 FROM task_attempts WHERE user_id=?)""",(row['id'],row['id'])).fetchone()
            total_xp=conn.execute('SELECT COALESCE(SUM(xp),0) xp FROM xp_events WHERE user_id=?',(row['id'],)).fetchone()['xp']
            conn.close()
            from datetime import date, timedelta
            today=date.today()
            by_day={x['day']:dict(x) for x in attempts}            by_xp={x['day']:x['xp'] for x in xp_days}
            daily=[]
            for i in range(29,-1,-1):
                d=(today-timedelta(days=i)).isoformat(); a=by_day.get(d,{})
                daily.append({'day':d,'attempts':a.get('attempts',0),'correct':a.get('correct',0),
                              'accuracy':round((a.get('correct',0)/a.get('attempts',1))*100) if a.get('attempts',0) else 0,
                              'avg_score':round(a.get('avg_score',0) or 0),'xp':by_xp.get(d,0)})
            def rate(r): return round((r['correct']/r['n']*100) if r['n'] else 0)
            improvement=rate(recent)-rate(first)
            score_improvement=round((recent['score'] or 0)-(first['score'] or 0))
            mastered=sum(1 for c in comps if c['status']=='mastered')
            avg_mastery=round(sum(c['mastery'] for c in comps)/len(comps)) if comps else 0
            milestones=[]
            for threshold,label in [(100,'100 XP gesammelt'),(500,'500 XP gesammelt'),(1000,'1.000 XP gesammelt'),(2500,'2.500 XP gesammelt'),(5000,'5.000 XP gesammelt')]:
                if total_xp>=threshold: milestones.append({'threshold':threshold,'label':label})
            if mastered: milestones.append({'threshold':mastered,'label':f'{mastered} Kompetenzen beherrscht'})
            self.send_json(200,{
                'summary':{'total_attempts':all_attempts['n'],'accuracy':rate(all_attempts),'avg_score':round(all_attempts['score'] or 0),
                           'total_xp':total_xp,'mastered_competencies':mastered,'total_competencies':len(comps),
                           'average_mastery':avg_mastery,'accuracy_change':improvement,'score_change':score_improvement},
                'comparison':{'first_attempts':first['n'],'first_accuracy':rate(first),'first_score':round(first['score'] or 0),
                              'recent_attempts':recent['n'],'recent_accuracy':rate(recent),'recent_score':round(recent['score'] or 0)},
                'daily':daily,'competencies':[dict(c) for c in comps],'milestones':milestones
            }); return

        if path == '/api/learning/history':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db()
            attempts=conn.execute("""SELECT a.id,a.task_id,a.answer,a.correct,a.feedback,a.error_type,a.score,a.next_hint,a.remediation,a.created_at,t.title,t.path,t.module,t.xp
                FROM task_attempts a JOIN learning_tasks t ON t.id=a.task_id
                WHERE a.user_id=? ORDER BY a.id DESC LIMIT 50""",(row['id'],)).fetchall()
            xp_events=conn.execute('SELECT id,action,xp,created_at FROM xp_events WHERE user_id=? ORDER BY id DESC LIMIT 30',(row['id'],)).fetchall()
            competencies=conn.execute('SELECT position,title,topic,mastery,status,attempts,correct_attempts,updated_at FROM learning_competencies WHERE user_id=? ORDER BY position',(row['id'],)).fetchall()
            errors=conn.execute("""SELECT error_type,SUM(occurrences) occurrences,MAX(last_seen) last_seen
                FROM error_profiles WHERE user_id=? GROUP BY error_type ORDER BY occurrences DESC,last_seen DESC""",(row['id'],)).fetchall()
            totals=conn.execute("""SELECT COUNT(*) attempts,COALESCE(SUM(correct),0) correct,COALESCE(AVG(score),0) avg_score,
                COUNT(DISTINCT task_id) distinct_tasks FROM task_attempts WHERE user_id=?""",(row['id'],)).fetchone()
            streak=0
            days=conn.execute('SELECT DISTINCT substr(created_at,1,10) day FROM task_attempts WHERE user_id=? ORDER BY day DESC LIMIT 60',(row['id'],)).fetchall()
            if days:
                from datetime import date,timedelta
                expected=date.fromisoformat(days[0]['day'])
                for d in days:
                    actual=date.fromisoformat(d['day'])
                    if actual==expected:
                        streak+=1; expected-=timedelta(days=1)
                    elif actual<expected:
                        break
            total_xp=conn.execute('SELECT COALESCE(SUM(xp),0) xp FROM xp_events WHERE user_id=?',(row['id'],)).fetchone()['xp']
            conn.close()
            self.send_json(200,{
                'summary':{'attempts':totals['attempts'],'correct':totals['correct'],'accuracy':round((totals['correct']/totals['attempts']*100) if totals['attempts'] else 0),
                           'avg_score':round(totals['avg_score'] or 0),'distinct_tasks':totals['distinct_tasks'],'streak_days':streak,'earned_xp':total_xp},
                'attempts':[dict(x) for x in attempts],
                'xp_events':[dict(x) for x in xp_events],
                'competencies':[dict(x) for x in competencies],
                'errors':[dict(x) for x in errors]
            }); return

        if path == '/api/learning/settings':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); pref=conn.execute('SELECT * FROM learning_preferences WHERE user_id=?',(row['id'],)).fetchone(); plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone(); conn.close()
            self.send_json(200, {'preference':dict(pref) if pref else None, 'plan':dict(plan) if plan else None}); return

        if path == '/api/learning/recommendation':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            if not plan:
                pref=conn.execute('SELECT * FROM learning_preferences WHERE user_id=?',(row['id'],)).fetchone()
                target=pref['target_level'] if pref else 'Fortgeschrittener Anfänger'
                custom=pref['custom_goal'] if pref and pref['custom_goal'] else row['goal']
                subject,path_name,steps=build_plan(row,target,custom)
                sync_plan_competencies(conn,row['id'],steps)
                conn.execute('INSERT INTO learning_plans(user_id,goal,subject,current_level,target_level,learning_mode,domain,plan_json) VALUES(?,?,?,?,?,?,?,?)',(row['id'],custom or 'Lernen',subject,row['level'] or 'Anfänger',target,pref['learning_mode'] if pref else 'guided',pref['domain'] if pref else subject,json.dumps(steps,ensure_ascii=False)))
                conn.commit(); plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            rec=adaptive_recommendation(conn,row['id'],plan); conn.close()
            self.send_json(200, rec or {'status':'none','task':None,'message':'Noch keine Empfehlung verfügbar.'})
            return

        if path == '/api/goalpredictor/live':
            try:
                from goalpredictor_api import live_matches
                return self.send_json(200, live_matches())
            except Exception as exc:
                return self.send_json(500, {'configured': False, 'matches': [], 'error': str(exc)})

        if path == '/api/goalpredictor/today':
            try:
                from goalpredictor_api import today_matches
                return self.send_json(200, today_matches())
            except Exception as exc:
                return self.send_json(500, {'configured': False, 'matches': [], 'error': str(exc)})

        if path.startswith('/api/goalpredictor/match/'):
            try:
                from goalpredictor_api import match_detail
                fixture_id=int(path.rsplit('/',1)[1])
                return self.send_json(200, match_detail(fixture_id))
            except Exception as exc:
                return self.send_json(500, {'configured': False, 'error': str(exc)})

        if path == '/': path = '/index.html'
        safe = os.path.normpath(path.lstrip('/'))
        full = os.path.join(STATIC, safe)
        if not full.startswith(STATIC) or not os.path.isfile(full):
            self.send_error(404); return
        ctype = 'text/html; charset=utf-8' if full.endswith('.html') else 'text/plain; charset=utf-8'
        data = open(full, 'rb').read(); self.send_response(200); self.send_header('Content-Type', ctype); self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)

    def do_POST(self):
        path = urlparse(self.path).path
        try: body = json_body(self)
        except Exception: self.send_json(400, {'error':'Ungültige Anfrage.'}); return

        if path == '/api/profile':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            name=clean(body.get('name'),80)
            if not name: return self.send_json(400, {'error':'Bitte einen Anzeigenamen eingeben.'})
            skills=split_tags(body.get('skills',[]))
            interests=split_tags(body.get('interests',[]))
            goals=split_tags(body.get('learning_goals',[]))
            conn=db()
            conn.execute('''UPDATE users SET name=?,about=?,goal=?,category=?,level=?,skills=?,language=? WHERE id=?''',(name,clean(body.get('about'),2000),clean(body.get('goal'),1000),clean(body.get('category'),100),clean(body.get('level'),50),', '.join(skills),clean(body.get('language'),10) or 'de',row['id']))
            set_profile_tags(conn,row['id'],interests,goals)
            conn.commit(); profile=profile_payload(conn,row['id']); matches=matching_users(conn,row['id']); conn.close()
            return self.send_json(200,{'profile':profile,'matches':matches,'message':'Profil gespeichert.'})

        if path == '/api/register':
            email = clean(body.get('email'), 254).lower(); password = str(body.get('password') or '')
            if not re.fullmatch(r'[^@\s]+@[^@\s]+\.[^@\s]+', email): return self.send_json(400, {'error':'Bitte eine gültige E-Mail-Adresse eingeben.'})
            if len(password) < 8: return self.send_json(400, {'error':'Das Passwort muss mindestens 8 Zeichen haben.'})
            name = clean(body.get('name'), 80)
            if not name: return self.send_json(400, {'error':'Bitte einen Anzeigenamen eingeben.'})
            conn = db()
            try:
                cur = conn.execute('''INSERT INTO users(email,password_hash,name,role,country,language,age_group,about,goal,category,level,skills)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (email, hash_password(password), name, clean(body.get('role'),20) or 'both', clean(body.get('country'),50) or 'Österreich', clean(body.get('language'),10) or 'de', clean(body.get('age'),20) or '18–24', clean(body.get('about')), clean(body.get('goal')), clean(body.get('category'),100) or 'Sonstiges', clean(body.get('level'),50) or 'Anfänger', clean(body.get('skills'),500)))
                uid = cur.lastrowid
                set_profile_tags(conn,uid,body.get('interests',[]),body.get('learning_goals',[]))
                conn.commit()
                row = conn.execute('SELECT * FROM users WHERE id=?',(uid,)).fetchone()
            except sqlite3.IntegrityError:
                conn.close(); return self.send_json(409, {'error':'Für diese E-Mail-Adresse existiert bereits ein Konto.'})
            token = secrets.token_urlsafe(32)
            conn.execute('INSERT OR REPLACE INTO sessions(token,user_id) VALUES(?,?)',(token,uid))
            conn.commit()
            conn.close()
            SESSIONS[token] = uid
            self.send_json(201, {'user':public_user(row)}, [f'session={token}; Max-Age=2592000; HttpOnly; SameSite=Lax; Path=/'])
            return

        if path == '/api/login':
            email = clean(body.get('email'),254).lower(); password = str(body.get('password') or '')
            conn=db(); row=conn.execute('SELECT * FROM users WHERE email=?',(email,)).fetchone()
            if not row:
                conn.close()
                return self.send_json(401, {'error':'Für diese E-Mail-Adresse wurde kein Konto gefunden.'})
            if not verify_password(password,row['password_hash']):
                conn.close()
                return self.send_json(401, {'error':'Das Passwort ist falsch. Bitte prüfe deine Eingabe.'})
            token=secrets.token_urlsafe(32)
            conn.execute('INSERT OR REPLACE INTO sessions(token,user_id) VALUES(?,?)',(token,row['id']))
            conn.commit()
            conn.close()
            SESSIONS[token]=row['id']
            self.send_json(200, {'user':public_user(row)}, [f'session={token}; Max-Age=2592000; HttpOnly; SameSite=Lax; Path=/'])
            return

        m=re.fullmatch(r'/api/users/(\d+)/(follow|followers)',path)
        if m:
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            target=int(m.group(1)); action=m.group(2)
            conn=db(); user=conn.execute('SELECT id,name,level,skills FROM users WHERE id=?',(target,)).fetchone()
            if not user: conn.close(); return self.send_json(404, {'error':'Nutzer nicht gefunden.'})
            if action=='followers':
                followers=conn.execute('SELECT u.id,u.name,u.level,u.skills FROM user_follows f JOIN users u ON u.id=f.follower_id WHERE f.following_id=? ORDER BY f.created_at DESC',(target,)).fetchall()
                conn.close(); return self.send_json(200,{'followers':[dict(x) for x in followers]})
            if target==row['id']:
                conn.close(); return self.send_json(400,{'error':'Du kannst dir nicht selbst folgen.'})
            exists=conn.execute('SELECT 1 FROM user_follows WHERE follower_id=? AND following_id=?',(row['id'],target)).fetchone()
            if exists:
                conn.execute('DELETE FROM user_follows WHERE follower_id=? AND following_id=?',(row['id'],target)); following=False
            else:
                conn.execute('INSERT INTO user_follows(follower_id,following_id) VALUES(?,?)',(row['id'],target)); following=True
            follower_count=conn.execute('SELECT COUNT(*) FROM user_follows WHERE following_id=?',(target,)).fetchone()[0]
            conn.commit(); conn.close()
            return self.send_json(200,{'following':following,'follower_count':follower_count,'user':dict(user)})

        if path == '/api/community/posts':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            title=clean(body.get('title'),160); content=clean(body.get('content'),4000); topic=clean(body.get('topic'),60) or 'Allgemein'; post_type=clean(body.get('post_type'),20) or 'question'
            if post_type not in ('question','discussion','tip'): return self.send_json(400, {'error':'Ungültiger Beitragstyp.'})
            if len(title)<4: return self.send_json(400, {'error':'Der Titel ist zu kurz.'})
            if len(content)<10: return self.send_json(400, {'error':'Der Beitrag braucht etwas mehr Inhalt.'})
            conn=db()
            used=conn.execute("SELECT COUNT(*) FROM xp_events WHERE user_id=? AND action='Community-Beitrag' AND date(created_at)=date('now')",(row['id'],)).fetchone()[0]
            cur=conn.execute('INSERT INTO community_posts(user_id,post_type,title,content,topic) VALUES(?,?,?,?,?)',(row['id'],post_type,title,content,topic))
            gained=5 if used<3 else 0
            if gained:
                conn.execute("INSERT INTO xp_events(user_id,action,xp) VALUES(?,?,?)",(row['id'],'Community-Beitrag',gained)); conn.execute('UPDATE users SET xp=xp+? WHERE id=?',(gained,row['id']))
            post=conn.execute('SELECT p.*,u.name FROM community_posts p JOIN users u ON u.id=p.user_id WHERE p.id=?',(cur.lastrowid,)).fetchone()
            conn.commit(); conn.close()
            self.send_json(201,{'post':dict(post),'gained':gained,'message':('Beitrag veröffentlicht · +5 XP' if gained else 'Beitrag veröffentlicht.')}); return

        m=re.fullmatch(r'/api/community/posts/(\d+)/(like|comments)',path)
        if m:
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            post_id=int(m.group(1)); action=m.group(2); conn=db()
            post=conn.execute('SELECT * FROM community_posts WHERE id=?',(post_id,)).fetchone()
            if not post: conn.close(); return self.send_json(404, {'error':'Beitrag nicht gefunden.'})
            if action=='like':
                exists=conn.execute('SELECT 1 FROM community_post_likes WHERE user_id=? AND post_id=?',(row['id'],post_id)).fetchone()
                if exists:
                    conn.execute('DELETE FROM community_post_likes WHERE user_id=? AND post_id=?',(row['id'],post_id)); conn.execute('UPDATE community_posts SET likes=MAX(0,likes-1) WHERE id=?',(post_id,)); liked=False
                else:
                    conn.execute('INSERT INTO community_post_likes(user_id,post_id) VALUES(?,?)',(row['id'],post_id)); conn.execute('UPDATE community_posts SET likes=likes+1 WHERE id=?',(post_id,)); liked=True
                likes=conn.execute('SELECT likes FROM community_posts WHERE id=?',(post_id,)).fetchone()['likes']; conn.commit(); conn.close()
                return self.send_json(200,{'liked':liked,'likes':likes})
            content=clean(body.get('content'),3000)
            if len(content)<2: conn.close(); return self.send_json(400,{'error':'Der Kommentar ist zu kurz.'})
            used=conn.execute("SELECT COUNT(*) FROM xp_events WHERE user_id=? AND action='Community-Antwort' AND date(created_at)=date('now')",(row['id'],)).fetchone()[0]
            cur=conn.execute('INSERT INTO community_comments(post_id,user_id,content) VALUES(?,?,?)',(post_id,row['id'],content))
            gained=10 if used<5 else 0
            if gained:
                conn.execute("INSERT INTO xp_events(user_id,action,xp) VALUES(?,?,?)",(row['id'],'Community-Antwort',gained)); conn.execute('UPDATE users SET xp=xp+? WHERE id=?',(gained,row['id']))
            comment=conn.execute('SELECT c.*,u.name,u.level,u.skills FROM community_comments c JOIN users u ON u.id=c.user_id WHERE c.id=?',(cur.lastrowid,)).fetchone(); conn.commit(); conn.close()
            return self.send_json(201,{'comment':dict(comment),'gained':gained,'message':('Antwort veröffentlicht · +10 XP' if gained else 'Antwort veröffentlicht.')})

        m=re.fullmatch(r'/api/community/comments/(\d+)/like',path)
        if m:
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            cid=int(m.group(1)); conn=db(); comment=conn.execute('SELECT * FROM community_comments WHERE id=?',(cid,)).fetchone()
            if not comment: conn.close(); return self.send_json(404,{'error':'Kommentar nicht gefunden.'})
            exists=conn.execute('SELECT 1 FROM community_comment_likes WHERE user_id=? AND comment_id=?',(row['id'],cid)).fetchone()
            if exists:
                conn.execute('DELETE FROM community_comment_likes WHERE user_id=? AND comment_id=?',(row['id'],cid)); conn.execute('UPDATE community_comments SET likes=MAX(0,likes-1) WHERE id=?',(cid,)); liked=False
            else:
                conn.execute('INSERT INTO community_comment_likes(user_id,comment_id) VALUES(?,?)',(row['id'],cid)); conn.execute('UPDATE community_comments SET likes=likes+1 WHERE id=?',(cid,)); liked=True
            likes=conn.execute('SELECT likes FROM community_comments WHERE id=?',(cid,)).fetchone()['likes']; conn.commit(); conn.close(); return self.send_json(200,{'liked':liked,'likes':likes})

        if path == '/api/xp':
            row = session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            action = clean(body.get('action'), 40)
            rewards = {
                'daily_checkin': ('Tages-Check-in', 5, 1),
                'learning_task': ('Lernaufgabe abgeschlossen', 20, 3),
                'helpful_answer': ('Hilfreiche Antwort', 15, 5),
                'project': ('Projektfortschritt', 50, 1),
            }
            if action not in rewards: return self.send_json(400, {'error':'Unbekannte XP-Aktion.'})
            label, amount, daily_limit = rewards[action]
            conn = db()
            used = conn.execute("SELECT COUNT(*) FROM xp_events WHERE user_id=? AND action=? AND date(created_at)=date('now')", (row['id'], action)).fetchone()[0]
            if used >= daily_limit:
                conn.close()
                return self.send_json(429, {'error':f'Das tägliche Limit für „{label}“ ist bereits erreicht.'})
            conn.execute('INSERT INTO xp_events(user_id,action,xp) VALUES(?,?,?)', (row['id'], action, amount))
            conn.execute('UPDATE users SET xp=xp+? WHERE id=?', (amount, row['id']))
            new_row = conn.execute('SELECT * FROM users WHERE id=?', (row['id'],)).fetchone()
            higher = conn.execute('SELECT COUNT(*) FROM users WHERE xp > ?', (new_row['xp'],)).fetchone()[0]
            total = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
            conn.commit(); conn.close()
            xp = new_row['xp']; level = xp // 100 + 1
            current_level_xp = (level - 1) * 100
            progress = round((xp-current_level_xp)/100*100)
            self.send_json(200, {'message':f'+{amount} XP: {label}', 'user':public_user(new_row), 'gained':amount, 'rank':higher+1, 'total_users':total, 'level':level, 'next_level_xp':level*100, 'progress':progress})
            return

        if path == '/api/learning/settings':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            domain=clean(body.get('domain'),80) or 'Allgemein'; current=clean(body.get('current_level'),60) or 'Anfänger'; target=clean(body.get('target_level'),80) or 'Fortgeschrittener Anfänger'; mode=clean(body.get('learning_mode'),30) or 'guided'; custom=clean(body.get('custom_goal'),500)
            conn=db(); conn.execute('INSERT INTO learning_preferences(user_id,domain,current_level,target_level,learning_mode,custom_goal,updated_at) VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP) ON CONFLICT(user_id) DO UPDATE SET domain=excluded.domain,current_level=excluded.current_level,target_level=excluded.target_level,learning_mode=excluded.learning_mode,custom_goal=excluded.custom_goal,updated_at=CURRENT_TIMESTAMP',(row['id'],domain,current,target,mode,custom))
            subject,path_name,steps=build_plan(row,target,custom or row['goal'])
            sync_plan_competencies(conn,row['id'],steps)
            conn.execute('INSERT INTO learning_plans(user_id,goal,subject,current_level,target_level,learning_mode,domain,plan_json,current_step,updated_at) VALUES(?,?,?,?,?,?,?,?,1,CURRENT_TIMESTAMP) ON CONFLICT(user_id) DO UPDATE SET goal=excluded.goal,subject=excluded.subject,current_level=excluded.current_level,target_level=excluded.target_level,learning_mode=excluded.learning_mode,domain=excluded.domain,plan_json=excluded.plan_json,current_step=1,updated_at=CURRENT_TIMESTAMP',(row['id'],custom or row['goal'] or 'Lernen',subject,current,target,mode,domain,json.dumps(steps,ensure_ascii=False)))
            conn.commit(); plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone(); conn.close()
            self.send_json(200, {'message':'Lernpfad aktualisiert.','plan':dict(plan),'steps':steps}); return

        if path == '/api/curriculum/select':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            country=clean(body.get('country'),50) or 'Österreich'; school=clean(body.get('school_type'),60) or 'Mittelschule'; year=clean(body.get('year_level'),30) or '1. Klasse'; subject=clean(body.get('subject'),60) or 'Mathematik'
            conn=db(); cur=get_curriculum(conn,country,school,year,subject)
            if not cur: conn.close(); return self.send_json(404,{'error':'Für diese Auswahl ist noch kein Lernpfad hinterlegt.'})
            conn.execute('INSERT INTO user_curriculum(user_id,country,school_type,year_level,subject,curriculum_id) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET country=excluded.country,school_type=excluded.school_type,year_level=excluded.year_level,subject=excluded.subject,curriculum_id=excluded.curriculum_id',(row['id'],country,school,year,subject,cur['id']))
            conn.execute('UPDATE tutor_profiles SET subject=?,current_topic=?,updated_at=CURRENT_TIMESTAMP WHERE user_id=?',(subject,'Curriculum: '+subject,row['id']))
            conn.commit(); comps=[dict(x) for x in conn.execute('SELECT * FROM competencies WHERE curriculum_id=? ORDER BY position',(cur['id'],)).fetchall()]; conn.close()
            self.send_json(200,{'curriculum':dict(cur),'competencies':comps}); return

        if path == '/api/learning/diagnosis':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            conn=db(); plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            if not plan: conn.close(); return self.send_json(200,{'diagnosis':None})
            steps=json.loads(plan['plan_json']); sync_plan_competencies(conn,row['id'],steps)
            comp=next_learning_competency(conn,row['id'])
            result=learning_diagnosis(conn,row['id'],comp['position']) if comp else {'primary_error':None,'patterns':[],'message':'Alle Kompetenzen dieses Lernpfads sind beherrscht.'}
            result['competency']=dict(comp) if comp else None
            conn.close(); self.send_json(200,result); return

        if path == '/api/learning/submit':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            try: task_id=int(body.get('task_id'))
            except: return self.send_json(400, {'error':'Ungültige Aufgabe.'})
            answer=clean(body.get('answer'),5000)
            conn=db(); task=conn.execute('SELECT * FROM learning_tasks WHERE id=?',(task_id,)).fetchone()
            if not task: conn.close(); return self.send_json(404, {'error':'Aufgabe nicht gefunden.'})
            plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            comp=None
            if plan:
                steps=json.loads(plan['plan_json']); sync_plan_competencies(conn,row['id'],steps)
                path_tasks=conn.execute('SELECT position FROM learning_tasks WHERE path=? ORDER BY position',(task['path'],)).fetchall()
                comp_pos=task_competency_position(task,path_tasks,len(steps)); comp=conn.execute('SELECT * FROM learning_competencies WHERE user_id=? AND position=?',(row['id'],comp_pos)).fetchone()
            evaluation=llm_evaluate_answer(task,answer,comp,plan) if (comp and plan) else None
            if not evaluation:
                fallback=local_diagnostic_evaluation(task,answer,comp or {'title':task['title'],'topic':''})
                evaluation=fallback
            correct=bool(evaluation['correct']); feedback=evaluation['feedback']; error_type=evaluation.get('error_type','none'); score=int(evaluation.get('score',100 if correct else 0)); next_hint=evaluation.get('next_hint','')
            remediation='' if correct else build_remediation(error_type,task,answer,comp)
            if comp and not correct:
                update_error_profile(conn,row['id'],comp_pos,error_type,score)
            if comp:
                # weighted mastery: correct evidence raises confidence; repeated failure lowers it gently.
                comp=update_learning_competency(conn,row['id'],comp_pos,correct)
            conn.execute('INSERT INTO task_attempts(user_id,task_id,answer,correct,feedback,error_type,score,next_hint,remediation) VALUES(?,?,?,?,?,?,?,?,?)',(row['id'],task_id,answer,int(correct),feedback,error_type,score,next_hint,remediation))
            gained=0
            if correct:
                already=conn.execute('SELECT 1 FROM user_task_progress WHERE user_id=? AND task_id=?',(row['id'],task_id)).fetchone()
                if not already:
                    gained=task['xp']; conn.execute('INSERT INTO user_task_progress(user_id,task_id) VALUES(?,?)',(row['id'],task_id)); conn.execute('UPDATE users SET xp=xp+? WHERE id=?',(gained,row['id'])); conn.execute('INSERT INTO xp_events(user_id,action,xp) VALUES(?,?,?)',(row['id'],f'Lernaufgabe: {task["title"]}',gained))
                    if comp and comp['status']=='mastered':
                        nxt=next_learning_competency(conn,row['id'])
                        if nxt: conn.execute('UPDATE learning_plans SET current_step=?,updated_at=CURRENT_TIMESTAMP WHERE user_id=?',(nxt['position'],row['id']))
                    elif comp: conn.execute('UPDATE learning_plans SET current_step=?,updated_at=CURRENT_TIMESTAMP WHERE user_id=?',(comp['position'],row['id']))
            if correct and comp and comp['status']=='mastered':
                nxt=next_learning_competency(conn,row['id'])
                if nxt: conn.execute('UPDATE learning_plans SET current_step=?,updated_at=CURRENT_TIMESTAMP WHERE user_id=?',(nxt['position'],row['id']))
            new=conn.execute('SELECT * FROM users WHERE id=?',(row['id'],)).fetchone(); higher=conn.execute('SELECT COUNT(*) FROM users WHERE xp>?',(new['xp'],)).fetchone()[0]; total=conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
            recommendation=adaptive_recommendation(conn,row['id'],plan) if plan else None
            conn.commit(); conn.close()
            xp=new['xp']; level=xp//100+1; progress=round(((xp-(level-1)*100)/100)*100)
            self.send_json(200, {'correct':correct,'feedback':feedback,'gained':gained,'user':public_user(new),'rank':higher+1,'total_users':total,'level':level,'next_level_xp':level*100,'progress':progress,'competency':dict(comp) if comp else None,'recommendation':recommendation,'message':('🎉 Aufgabe gemeistert! +%s XP'%gained if gained else ('Weiter so – diese Aufgabe war bereits abgeschlossen.' if correct else 'Noch nicht ganz. Nutze die Mini-Lerneinheit und versuche es erneut.')),'diagnostic':{'score':score,'error_type':error_type,'next_hint':next_hint,'remediation':remediation,'llm':bool(llm_evaluate_answer if llm_enabled() else False)}})
            return

        if path == '/api/learning/complete':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            try: task_id=int(body.get('task_id'))
            except: return self.send_json(400, {'error':'Ungültige Aufgabe.'})
            conn=db(); task=conn.execute('SELECT * FROM learning_tasks WHERE id=?',(task_id,)).fetchone()
            if not task: conn.close(); return self.send_json(404, {'error':'Aufgabe nicht gefunden.'})
            already=conn.execute('SELECT 1 FROM user_task_progress WHERE user_id=? AND task_id=?',(row['id'],task_id)).fetchone()
            if already: conn.close(); return self.send_json(409, {'error':'Diese Aufgabe wurde bereits abgeschlossen.'})
            conn.execute('INSERT INTO user_task_progress(user_id,task_id) VALUES(?,?)',(row['id'],task_id))
            conn.execute('UPDATE users SET xp=xp+? WHERE id=?',(task['xp'],row['id']))
            conn.execute('INSERT INTO xp_events(user_id,action,xp) VALUES(?,?,?)',(row['id'],f'Lernpfad: {task["title"]}',task['xp']))
            new=conn.execute('SELECT * FROM users WHERE id=?',(row['id'],)).fetchone(); higher=conn.execute('SELECT COUNT(*) FROM users WHERE xp>?',(new['xp'],)).fetchone()[0]; total=conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]; conn.commit(); conn.close()
            xp=new['xp']; level=xp//100+1; progress=round(((xp-(level-1)*100)/100)*100)
            self.send_json(200, {'message':f'+{task["xp"]} XP', 'user':public_user(new),'gained':task['xp'],'rank':higher+1,'total_users':total,'level':level,'progress':progress})
            return

        if path == '/api/tutor':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            message=clean(body.get('message'),2000)
            if not message: return self.send_json(400, {'error':'Schreibe eine Frage an deinen KI-Nachhilfelehrer.'})
            mode=clean(body.get('mode'),30) or 'coach'
            conn=db()
            profile=conn.execute('SELECT * FROM tutor_profiles WHERE user_id=?',(row['id'],)).fetchone()
            if not profile:
                conn.execute('INSERT INTO tutor_profiles(user_id,subject,level) VALUES(?,?,?)',(row['id'],'Python',row['level'] or 'Anfänger')); conn.commit()
                profile=conn.execute('SELECT * FROM tutor_profiles WHERE user_id=?',(row['id'],)).fetchone()
            recent=conn.execute("SELECT role,message FROM tutor_messages WHERE user_id=? ORDER BY id DESC LIMIT 8",(row['id'],)).fetchall()
            uc=conn.execute('SELECT * FROM user_curriculum WHERE user_id=?',(row['id'],)).fetchone()
            if not uc:
                default_cur=get_curriculum(conn,'Österreich','Mittelschule','1. Klasse','Mathematik')
                conn.execute('INSERT INTO user_curriculum(user_id,country,school_type,year_level,subject,curriculum_id) VALUES(?,?,?,?,?,?)',(row['id'],'Österreich','Mittelschule','1. Klasse','Mathematik',default_cur['id']))
                conn.commit()
            lower=message.lower()
            topic=profile['current_topic']
            mastery=profile['mastery']
            plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            if plan is None:
                subject,path,steps=build_plan(row); conn.execute('INSERT INTO learning_plans(user_id,goal,subject,current_level,plan_json) VALUES(?,?,?,?,?)',(row['id'],row['goal'] or 'Lernen',subject,row['level'] or 'Anfänger',json.dumps(steps,ensure_ascii=False))); conn.commit(); plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            cur,comps=curriculum_context(conn,row['id'])
            adaptive=adaptive_recommendation(conn,row['id'],plan)
            if comps:
                next_comp=next((c for c in comps if c['status']!='mastered'),comps[0])
                if not profile['current_topic'].startswith('Curriculum:'):
                    topic=next_comp['title']
                curriculum_hint=f" Dein aktueller Lernplan ist {cur['title']}. Die nächste Kompetenz ist: {next_comp['title']}."
            else:
                curriculum_hint=''
            # Use the real LLM when configured. The local coaching engine below remains
            # the fallback so the tutor still works without an API key.
            llm_reply=None
            if llm_enabled():
                tutor_payload={
                    'student': {'name':row['name'],'level':row['level'],'goal':row['goal'] or ''},
                    'message': message,
                    'mode': mode,
                    'topic': topic,
                    'mastery': mastery,
                    'curriculum': dict(cur) if cur else None,
                    'next_competency': dict(next_comp) if comps else None,
                    'adaptive_task': dict(adaptive['task']) if adaptive and adaptive.get('task') else None,
                    'recent_messages':[dict(x) for x in recent[-6:]]
                }
                llm_reply=llm_text(
                    "Du bist der persönliche KI-Nachhilfelehrer von NextStep. "
                    "Hilf einem Lernenden auf seinem aktuellen Niveau. Antworte auf Deutsch, "
                    "freundlich und konkret. Erkläre verständlich, stelle Rückfragen und gib "
                    "bei Aufgaben bevorzugt Hinweise statt sofort die vollständige Lösung. "
                    "Berücksichtige den Lernplan, die aktuelle Kompetenz und die letzte Unterhaltung. "
                    "Wenn der Lernende eine Aufgabe lösen soll, lass ihn zuerst selbst versuchen.",
                    tutor_payload, 1200
                )
            # A deterministic coaching engine for the local prototype. It is intentionally designed
            # around questions and hints, not simply dumping solutions.
            diagnostic_step=profile['diagnostic_step'] if 'diagnostic_step' in profile.keys() else 0
            if any(x in lower for x in ['diagnose starten','starte diagnose','diagnose']):
                topic='Einstiegsdiagnose'; diagnostic_step=1
                reply=('🧪 **Kurze Einstiegsdiagnose**\n\nWas gibt dieses Python-Programm aus?\n\n`x = 3`\n`y = 2`\n`print(x + y)`\n\nAntworte nur mit deiner Vermutung. Ich passe danach die nächsten Aufgaben an dein Niveau an.')
            elif diagnostic_step == 1:
                topic='Einstiegsdiagnose'; diagnostic_step=2
                if lower.strip() in ['5','fünf','es gibt 5 aus','5 aus']:
                    mastery=max(mastery,20); reply=('✅ Richtig! `x + y` ergibt `5`. Du hast die grundlegende Verwendung von Variablen und Rechenoperatoren verstanden.\n\n**Nächster Test:** Was gibt `name = "Alex"` und danach `print(name)` aus?')
                else:
                    mastery=max(mastery,5); reply=('Fast – schauen wir es gemeinsam an. `x` enthält `3` und `y` enthält `2`. Bei `x + y` werden beide Werte addiert.\n\nWas ist also `3 + 2`?')
            elif diagnostic_step == 2:
                topic='Einstiegsdiagnose'; diagnostic_step=0
                if 'alex' in lower or 'alex' == lower.strip() or 'text' in lower or 'alex aus' in lower:
                    mastery=max(mastery,40); reply=('🎉 Diagnose abgeschlossen. Du hast die Grundlagen bereits gut erfasst. Ich würde dich als **fortgeschrittenen Anfänger** einordnen. Als Nächstes passen Bedingungen und kleine Programmieraufgaben gut zu dir.')
                else:
                    mastery=max(mastery,25); reply=('Danke! Du hast die Diagnose abgeschlossen. Ich würde mit den Grundlagen beginnen und die ersten Aufgaben bewusst kurz halten. Danach erhöhen wir die Schwierigkeit Schritt für Schritt.')
            elif any(x in lower for x in ['variable','variablen']):
                topic='Variablen'
                reply=('Stell dir eine Variable wie eine beschriftete Box vor. In `name = "Kobold"` heißt die Box `name` und enthält den Text. '
                       'Bevor ich mehr erkläre: **Was erwartest du bei `print(name)` als Ausgabe?**')
                mastery=max(mastery,10)
            elif 'print' in lower:
                topic='Ausgabe mit print()'
                reply=('`print()` zeigt einen Wert an. Wir machen es wie im Unterricht: erst du, dann ich. '
                       'Schreibe eine Zeile Python, die `Hallo NextStep!` ausgibt. Schick mir deinen Code – ich prüfe ihn.')
            elif 'if' in lower or 'beding' in lower:
                topic='Bedingungen'
                reply=('Bei `if` entscheidet Python, ob eine Bedingung wahr ist. **Hinweis:** Eine Bedingung kann zum Beispiel `alter >= 18` sein. '
                       'Versuche selbst eine Zeile zu schreiben, die bei `alter >= 18` den Text `Volljährig` ausgibt.')
            elif any(x in lower for x in ['fehler','error','funktioniert nicht','bug']):
                topic='Fehler verstehen'
                reply=('Wir debuggen gemeinsam. Schicke mir **den vollständigen Code** und – wenn vorhanden – **die genaue Fehlermeldung**. '
                       'Ich werde zuerst erklären, *warum* der Fehler entsteht, und dir dann einen kleinen Hinweis geben.')
            elif any(x in lower for x in ['schwer','schwierig']):
                topic='Herausforderung'
                reply=('Dann erhöhen wir die Schwierigkeit. 🚀 **Challenge:** Schreibe ein kleines Programm mit zwei Zahlen `a` und `b`, das ausgibt, welche Zahl größer ist. '
                       'Versuche es ohne Musterlösung. Wenn du festhängst, antworte einfach mit `Hinweis 1`.')
            elif 'hinweis 1' in lower:
                reply=('💡 **Hinweis 1:** Du brauchst eine `if`-Bedingung. Überlege: Was muss gelten, damit `a` größer als `b` ist? Schreibe nur diese Bedingung.')
            elif 'hinweis 2' in lower:
                reply=('💡 **Hinweis 2:** Wenn `a > b` wahr ist, kannst du `a` als größere Zahl ausgeben. Danach brauchst du noch einen Fall für `b` – und eventuell einen Fall, in dem beide gleich sind.')
            elif any(x in lower for x in ['aufgabe','quiz','teste mich','test']):
                topic='Diagnoseaufgabe'
                reply=('🧪 **Mini-Diagnose:** Was gibt dieses Programm aus?\n\n`name = "Alex"`\n`print("Hallo " + name)`\n\nAntworte nur mit deiner Vermutung. Danach erkläre ich dir, ob sie stimmt und warum.')
            elif any(x in lower for x in ['ich verstehe','verstehe ich nicht','keine ahnung','weiß nicht']):
                reply=('Das ist völlig in Ordnung. Wir gehen einen Schritt zurück. 😊 Sag mir, welcher Teil unklar ist: **1 = Begriff, 2 = Code, 3 = Fehlermeldung, 4 = Aufgabe**. '
                       'Dann passe ich die Erklärung an dich an.')
            else:
                reply=('Ich begleite dich Schritt für Schritt. Du kannst mir **eine Frage, deinen Code, eine Fehlermeldung oder eine eigene Lösung** schicken. '
                       'Wenn du möchtest, kann ich dich auch mit einer Aufgabe testen. Ich versuche zuerst mit Fragen und Hinweisen zu helfen, statt dir sofort die Lösung zu geben.')
                if adaptive and adaptive.get('status')=='ready':
                    reply += '\n\n🎯 **Dein nächster Lernschritt:** '+adaptive['task']['title']+'\n'+adaptive['reason']+'\nAktueller Kompetenzstand: '+str(adaptive['mastery'])+' %.'
            if llm_reply:
                reply=llm_reply
            if mode == 'explain' and not llm_reply:
                reply='Ich erkläre es dir zuerst einfach und danach mit einem Beispiel. '+reply
            elif mode == 'challenge':
                reply='🎯 Challenge-Modus: '+reply
            if curriculum_hint and not any(k in lower for k in ['diagnose','variable','print','if','fehler','schwer','hinweis','aufgabe','quiz','verstehe']):
                reply += curriculum_hint
            conn.execute('INSERT INTO tutor_messages(user_id,role,message) VALUES(?,?,?)',(row['id'],'user',message))
            conn.execute('INSERT INTO tutor_messages(user_id,role,message) VALUES(?,?,?)',(row['id'],'assistant',reply))
            delta = 8 if diagnostic_step == 0 and any(x in lower for x in ['richtig','ja','verstanden','funktioniert','gelöst']) else 2
            new_mastery=min(100,mastery+delta)
            conn.execute("UPDATE tutor_profiles SET current_topic=?, mastery=?, diagnostic_step=?, updated_at=CURRENT_TIMESTAMP WHERE user_id=?",(topic,new_mastery,diagnostic_step,row['id']))
            updated_plan=conn.execute('SELECT * FROM learning_plans WHERE user_id=?',(row['id'],)).fetchone()
            updated_recommendation=adaptive_recommendation(conn,row['id'],updated_plan) if updated_plan else None
            conn.commit(); conn.close()
            self.send_json(200, {'reply':reply,'topic':topic,'mastery':new_mastery,'mode':mode,'adaptation':('Die nächsten Aufgaben werden an deinen Lernstand angepasst.' if new_mastery>=40 else 'Wir bauen zuerst deine Grundlagen weiter aus.'),'learning_recommendation':updated_recommendation})
            return

        if path == '/api/profile':
            row=session_user(self)
            if not row: return self.send_json(401, {'error':'Nicht eingeloggt.'})
            fields=['name','role','country','language','age','about','goal','category','level','skills']
            values={f:clean(body.get(f),5000) for f in fields}
            if not values['name']: return self.send_json(400, {'error':'Der Anzeigename darf nicht leer sein.'})
            conn=db(); conn.execute('''UPDATE users SET name=?,role=?,country=?,language=?,age_group=?,about=?,goal=?,category=?,level=?,skills=? WHERE id=?''', (values['name'],values['role'] or 'both',values['country'] or 'Österreich',values['language'] or 'de',values['age'] or '18–24',values['about'],values['goal'],values['category'] or 'Sonstiges',values['level'] or 'Anfänger',values['skills'],row['id'])); conn.commit(); row=conn.execute('SELECT * FROM users WHERE id=?',(row['id'],)).fetchone(); conn.close()
            self.send_json(200, {'user':public_user(row)}); return

        if path == '/api/logout':
            cookie=self.headers.get('Cookie',''); token=None
            for part in cookie.split(';'):
                if part.strip().startswith('session='): token=part.strip().split('=',1)[1]
            if token:
                SESSIONS.pop(token,None)
                conn=db(); conn.execute('DELETE FROM sessions WHERE token=?',(token,)); conn.commit(); conn.close()
            self.send_json(200, {'ok':True}, ['session=; Max-Age=0; HttpOnly; SameSite=Lax; Path=/']); return

        self.send_json(404, {'error':'Nicht gefunden.'})

    def log_message(self, fmt, *args):        print('%s - %s' % (self.address_string(), fmt%args))


if __name__ == '__main__':
    init_db()
    port=int(os.getenv('PORT', os.getenv('NEXTSTEP_PORT','8000')))
    print(f'NextStep läuft auf http://0.0.0.0:{port}')
    ThreadingHTTPServer(('0.0.0.0',port), Handler).serve_forever()