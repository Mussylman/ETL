/* ── ETL Config App JS ── */

// Tabs
function switchTab(tabId) {
    const btn = document.querySelector(`[data-tab="tab-${tabId}"]`) || document.querySelector(`[data-tab="${tabId}"]`);
    if (!btn) return;
    const group = btn.closest('.tabs').dataset.tabGroup || 'default';
    document.querySelectorAll(`.tabs[data-tab-group="${group}"] .tab`).forEach(t => t.classList.remove('active'));
    document.querySelectorAll(`.tab-panel[data-tab-group="${group}"]`).forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    const panel = document.getElementById(btn.dataset.tab);
    if (panel) panel.classList.add('active');
}

document.querySelectorAll('[data-tab]').forEach(btn => {
    btn.addEventListener('click', () => {
        switchTab(btn.dataset.tab);
        // Update hash without scrolling
        history.replaceState(null, '', '#' + btn.dataset.tab.replace('tab-', ''));
    });
});

// Open tab from URL hash (e.g. #columns, #status)
if (window.location.hash) {
    const tabName = window.location.hash.slice(1);
    switchTab(tabName);
}

// Delete confirmation modal
function showDeleteModal(action, name) {
    const modal = document.getElementById('deleteModal');
    if (!modal) return;
    modal.querySelector('.delete-name').textContent = name || '';
    modal.querySelector('form').action = action;
    modal.classList.add('show');
}

function hideDeleteModal() {
    const modal = document.getElementById('deleteModal');
    if (modal) modal.classList.remove('show');
}

// 1C Autocomplete
function initAutocomplete(inputId, onSelect) {
    const input = document.getElementById(inputId);
    if (!input) return;

    const wrap = input.closest('.autocomplete-wrap');
    let listEl = wrap.querySelector('.autocomplete-list');
    if (!listEl) {
        listEl = document.createElement('div');
        listEl.className = 'autocomplete-list';
        wrap.appendChild(listEl);
    }

    let timer = null;

    input.addEventListener('input', () => {
        clearTimeout(timer);
        const q = input.value.trim();
        if (q.length < 2) {
            listEl.classList.remove('show');
            return;
        }
        timer = setTimeout(() => {
            listEl.innerHTML = '<div class="autocomplete-empty"><span class="spinner" style="width:14px;height:14px;border-width:2px;vertical-align:middle;margin-right:6px"></span> Поиск...</div>';
            listEl.classList.add('show');
            fetch(`/api/1c/search?q=${encodeURIComponent(q)}`)
                .then(r => r.json())
                .then(items => {
                    // items = [{table_name, table_name_sql}, ...]
                    if (!items.length) {
                        listEl.innerHTML = '<div class="autocomplete-empty">Ничего не найдено для "' + q + '". Попробуйте короче (напр. "Продажи", "ЧекККМ")</div>';
                        listEl.classList.add('show');
                        return;
                    }
                    listEl.innerHTML = items.map(item =>
                        `<div class="autocomplete-item" data-name="${item.table_name}" data-sql="${item.table_name_sql}">
                            ${item.table_name}
                            <span class="sql-name">${item.table_name_sql}</span>
                        </div>`
                    ).join('');
                    listEl.classList.add('show');
                    listEl.querySelectorAll('.autocomplete-item').forEach(el => {
                        el.addEventListener('click', () => {
                            input.value = el.dataset.name;
                            listEl.classList.remove('show');
                            // Auto-fill mssql_table with SQL name
                            const sqlField = document.getElementById('mssql_table');
                            if (sqlField) sqlField.value = el.dataset.sql;
                            if (onSelect) onSelect(el.dataset.name, el.dataset.sql);
                        });
                    });
                })
                .catch(() => listEl.classList.remove('show'));
        }, 300);
    });

    document.addEventListener('click', (e) => {
        if (!wrap.contains(e.target)) {
            listEl.classList.remove('show');
        }
    });
}

// Fetch 1C table structure and populate mssql_table field
function fetch1CStructure(tableName) {
    return fetch(`/api/1c/structure?table=${encodeURIComponent(tableName)}`)
        .then(r => r.json())
        .then(data => {
            if (data.length > 0) {
                const info = data[0];
                const sqlField = document.getElementById('mssql_table');
                if (sqlField) sqlField.value = info.table_name_sql;
                return info;
            }
            return null;
        });
}

// Load columns from 1C for column picker
function loadColumnsFrom1C(srcId) {
    const source = document.getElementById('source_1c_name');
    if (!source || !source.value) return;

    const btn = document.getElementById('loadColumnsBtn');
    const picker = document.getElementById('columnPicker');
    if (btn) btn.innerHTML = '<span class="spinner"></span> Loading...';

    fetch(`/api/1c/structure?table=${encodeURIComponent(source.value)}`)
        .then(r => r.json())
        .then(data => {
            if (!data.length || !data[0].fields) {
                if (picker) picker.innerHTML = '<div class="empty-state"><p>No columns found</p></div>';
                if (btn) btn.textContent = 'Load Columns from 1C';
                return;
            }
            const fields = data[0].fields;
            if (picker) {
                picker.innerHTML = fields.map((f, i) => {
                    // MSSQL columns have _ prefix
                    // Special cases: Recorder → _RecorderRRef, ID → _IDRRef
                    const sqlName = f.field_name_sql;
                    const specialMap = {'Recorder': '_RecorderRRef', 'ID': '_IDRRef'};
                    const mssqlCol = specialMap[sqlName] || ('_' + sqlName);
                    return `
                    <div class="column-picker-item">
                        <input type="checkbox" id="col_${i}" data-sql="${mssqlCol}" data-name="${f.field_name}">
                        <span class="col-name">${f.field_name}</span>
                        <span class="col-sql">${mssqlCol}</span>
                        <input type="text" placeholder="target_column" id="target_${i}" value="${sqlName.toLowerCase()}" style="width:140px">
                        <select id="type_${i}" style="width:110px;padding:4px 8px;font-size:13px">
                            <option value="varchar">varchar</option>
                            <option value="text">text</option>
                            <option value="integer">integer</option>
                            <option value="bigint">bigint</option>
                            <option value="numeric">numeric</option>
                            <option value="boolean">boolean</option>
                            <option value="uuid">uuid</option>
                            <option value="timestamp">timestamp</option>
                            <option value="date">date</option>
                            <option value="jsonb">jsonb</option>
                            <option value="bytea">bytea</option>
                        </select>
                    </div>
                `}).join('');
                picker.style.display = 'block';
                document.getElementById('saveColumnsBtn').style.display = 'inline-flex';
            }
            if (btn) btn.textContent = 'Load Columns from 1C';
        })
        .catch(() => {
            if (btn) btn.textContent = 'Load Columns from 1C';
        });
}

// Save selected columns
function saveSelectedColumns(sourceId) {
    const picker = document.getElementById('columnPicker');
    if (!picker) return;

    const mappings = [];
    picker.querySelectorAll('.column-picker-item').forEach((item, i) => {
        const cb = item.querySelector(`#col_${i}`);
        if (cb && cb.checked) {
            const targetType = document.getElementById(`type_${i}`).value || null;
            // Auto-assign transform based on target type
            let transform = null;
            if (targetType === 'uuid') transform = 'binary_to_uuid';
            else if (targetType === 'timestamp' || targetType === 'date') transform = 'fix_year';
            mappings.push({
                source_column: cb.dataset.sql,
                target_column: document.getElementById(`target_${i}`).value || cb.dataset.sql.toLowerCase(),
                target_type: targetType,
                transform_type: transform,
                onec_name: cb.dataset.name || null,
            });
        }
    });

    if (!mappings.length) {
        alert('Select at least one column');
        return;
    }

    const btn = document.getElementById('saveColumnsBtn');
    if (btn) btn.innerHTML = '<span class="spinner"></span> Saving...';

    fetch(`/sources/${sourceId}/mappings/batch`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({mappings}),
    })
        .then(r => r.json())
        .then(data => {
            if (data.ok) {
                window.location.reload();
            }
        })
        .catch(() => {
            if (btn) btn.textContent = 'Save Selected';
            alert('Error saving columns');
        });
}

// Select/deselect all checkboxes in column picker
function toggleAllColumns(checked) {
    document.querySelectorAll('#columnPicker input[type="checkbox"]').forEach(cb => {
        cb.checked = checked;
    });
}
