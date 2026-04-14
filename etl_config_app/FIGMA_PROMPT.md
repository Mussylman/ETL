# Промпт для Claude Code с Figma MCP

Закрой предыдущую сессию. В новой сессии отправь:

---

Используй Figma MCP tools чтобы СОЗДАТЬ дизайн прямо в Figma (НЕ скриншоты, НЕ headless browser). Используй инструменты figma_create_rectangle, figma_create_text, figma_create_frame и т.д. для рисования UI элементов напрямую в Figma.

Создай новый файл или используй существующий файл Figma. Нарисуй дизайн для ETL Config UI.

Начни с ОДНОЙ страницы — **Registers List**:

1. Создай фрейм 1440×900 с названием "Registers List"
2. Нарисуй sidebar слева (260px шириной, цвет заливки `#1E1B4B`):
   - Текст "ETL Config" белый, bold, 20px
   - Текст "Data Pipeline Manager" цвет `#A5B4FC`, 11px
   - Пункт меню "Registers" с hover фоном `#312E81`
3. Основной контент справа (фон `#F8FAFC`):
   - Topbar белый с breadcrumb "ETL Config / Registers"
   - Заголовок "ETL Registers" слева, кнопка "+ New Register" справа (фон `#4F46E5`, белый текст, radius 8)
   - 3 карточки регистров в ряд:
     - Белый фон, border `#E2E8F0`, radius 12
     - Название жирным "Продажи", badge "Active" зелёный (`#DCFCE7`, текст `#16A34A`)
     - Код серым моноширинным "sales_register"
     - Внизу: "Mode: incremental" серым

Используй ТОЛЬКО Figma MCP tools. НЕ запускай браузер, НЕ делай скриншоты, НЕ пиши Python скрипты.
