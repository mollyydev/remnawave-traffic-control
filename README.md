# Remnawave Traffic Control

Сервис для изолированного учета и контроля трафика **белых списков** пользователей панели Remnawave.

- Учет только выбранных нод (`WHITELISTS_NODE_IDS`);
- Расчет трафика **on-demand** через PostgreSQL Remnawave (без Redis);
- Автоматическое управление доступом в whitelist squad при исчерпании лимита и пополнении;
- Поддержка запуска на одном сервере с Remnawave или на отдельном сервере (standalone).

## Документация

Полная инструкция по установке и API справочник:
👉 **[itdocumentation.md](itdocumentation.md)**

### Быстрый старт:

- **На одном сервере с Remnawave:** используйте `docker-compose.yml` (подключение через сеть `remnawave-network`).
- **На отдельном сервере:** используйте `docker-compose.standalone.yml` (автоматически поднимает изолированный PostgreSQL и сервис).