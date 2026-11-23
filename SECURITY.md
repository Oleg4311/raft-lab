# Политика безопасности

## Как сообщить об уязвимости

Пожалуйста, **не создавайте публичный issue**. Сообщите приватно через GitHub: вкладка **Security** → **Report a vulnerability**
([создать сообщение](https://github.com/Oleg4311/raft-lab/security/advisories/new)).

Укажите, что именно не так, как это воспроизвести и какая версия затронута. Я постараюсь ответить в течение недели, но это проект одного человека, поэтому сроки не гарантирую.

## Поддерживаемые версии

Исправления выходят для последней версии: ветка `main` и последний релиз.

## Что считается уязвимостью

Это учебный и исследовательский проект: сетевых служб в нём нет, симулятор работает в одном процессе. Интересны: ошибки в алгоритме, при которых нарушаются свойства безопасности Raft на корректной реализации (приложите зерно и параметры прогона), ложные «успехи» проверок (нарушение, которое симулятор должен был поймать, но пропустил), зависание или аварийное завершение `raftlab` на корректных аргументах, XSS и внедрение HTML в просмотрщике через записи сценариев, подмена выпускаемых файлов.

## Что вне области

Отсутствие снимков и смены состава кластера, Pre-Vote и CheckQuorum (описано в README), уже известные проблемы в зависимостях (их закрывает Dependabot) и отчёты без способа воспроизведения.

---

## English

Please report vulnerabilities privately via GitHub Security Advisories (**Security** → **Report a vulnerability**) and do not open a public issue. This is a research project with no network services: the simulator runs in one process. In scope: algorithm errors that violate Raft safety on the correct implementation (include the seed and parameters), false negatives in the checkers, crashes or hangs of `raftlab` on valid arguments, HTML injection in the viewer through trace files, tampering with release artifacts. Out of scope: documented missing features (snapshots, membership changes, Pre-Vote, CheckQuorum), known dependency issues.
