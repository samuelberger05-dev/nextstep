# NextStep v21.4 – Wissens-Matching

## Neu
- Wissens-Matching aus NextStep-Profil, GitHub und freigegebenen Reddit-Communities
- Richtungsbezogene Signale:
  - kann dir helfen
  - du kannst helfen
  - gemeinsame Interessen
  - GitHub passt zu Lernzielen
  - gemeinsame Reddit-Communities
- nachvollziehbare Match-Gründe statt eines undurchsichtigen Scores
- konservative Alias-Erkennung für Begriffe wie Python/Programmieren, JS/JavaScript und ML/Machine Learning
- GitHub-Projekte speichern zusätzlich öffentliche Topics
- Profiloberfläche zeigt die verwendeten Match-Signale

## Datenschutzprinzip
GitHub- und Reddit-Daten werden nur als zusätzliche Matching-Signale verwendet. Reddit-Communities werden nicht automatisch zu NextStep-Interessen. Die Reddit-Auswahl muss vom Nutzer ausdrücklich freigegeben werden.

## Hinweis für Produktion
OAuth-Tokens sind in dieser lokalen Entwicklungsfassung noch nicht für einen öffentlichen Produktionsbetrieb gehärtet. Vor Deployment müssen Token-Verschlüsselung, Secret Management, HTTPS, CSRF-/Session-Härtung und Datenlöschung ergänzt werden.
