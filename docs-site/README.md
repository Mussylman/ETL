# Data Platform — handover-портал

Один внутренний сайт, три раздела: `/` (01 Как устроено), `/operations` (02 Как сопровождать),
`/analytics` (03 Данные и отчёты). Статический: HTML/CSS + немного vanilla JS, схемы — inline SVG, без CDN.

```bash
# сборка: live snapshot из production (READ ONLY: etl_meta, system.parts, Airflow) + страницы в dist/
venv/bin/python3 docs-site/build.py
venv/bin/python3 docs-site/build.py --offline     # без production — последний content/generated/snapshot.json

# сервер — только внутренний адрес (0.0.0.0 отклоняется)
systemctl --user status docs-site                  # unit: docs-site.service (копия в этом каталоге)
systemctl --user restart docs-site                 # после сборки не нужен: файлы отдаются с диска
systemctl --user stop docs-site
```

- Контент: `templates/` (страницы), `content/*.json` (правила, плейбуки, команды, каталог, решения),
  `static/svg/` (схемы, общая семантика цветов в `static/site.css`).
- Live snapshot недоступен → сборка не падает, в шапке «Live config unavailable» + время snapshot.
- Секретов на портале нет: креды — в Airflow connections, `~/.config/etl_config/prod.env`, `ch_admin.xml`.
- Автозапуск после reboot требует `sudo loginctl enable-linger dev` (без linger user-сервис живёт,
  пока у dev есть сессия).
