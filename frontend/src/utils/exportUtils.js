/**
 * exportUtils.js — Production-grade site-wide export engine.
 * Excel / PDF / CSV, all lazy-loaded.
 *
 * Key fixes in this version:
 * - Proper number formatting (no scientific notation) via cell.z in xlsx
 * - Correct autoTable(doc, {...}) function pattern for jspdf-autotable
 * - CSV metadata header block
 * - Descriptive filenames without brand prefix
 */

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const ROW_LIMITS = { excel: 20000, pdf: 5000, csv: 50000 };
const FORMULA_PREFIXES = ['=', '+', '-', '@'];

// Number format codes for xlsx cells (prevents scientific notation)
const NUM_FMTS = {
    integer: '#,##0',        // e.g. 1,26,66,17,627
    decimal2: '#,##0.00',     // e.g. 381,890.91
    percent2: '0.00',         // e.g. 16.62  (stored raw, label says %)
    price: '#,##0.00',     // e.g. 304.70
};

// ─────────────────────────────────────────────────────────────────────────────
// 1. sanitizeData
// ─────────────────────────────────────────────────────────────────────────────

export function sanitizeData(data) {
    if (!Array.isArray(data)) return [];
    return data.map(row => {
        const clean = {};
        for (const [key, value] of Object.entries(row)) {
            let v = value;
            if (v === null || v === undefined || (typeof v === 'number' && isNaN(v))) {
                v = '';
            }
            if (typeof v === 'string' && FORMULA_PREFIXES.some(p => v.startsWith(p))) {
                v = "'" + v;
            }
            clean[key] = v;
        }
        return clean;
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. formatColumnValue
// ─────────────────────────────────────────────────────────────────────────────

export function formatColumnValue(value, columnConfig = {}) {
    const { exportFormat, formatter } = columnConfig;
    if (value === null || value === undefined || value === '') return '';
    if (exportFormat === 'numeric') {
        const num = Number(value);
        return isNaN(num) ? '' : num; // raw number — xlsx formats it
    }
    if (typeof formatter === 'function') return formatter(value) ?? '';
    return String(value);
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. generateFileName — no brand prefix
// ─────────────────────────────────────────────────────────────────────────────

export function generateFileName({ page, filters = {}, ext }) {
    const parts = [page];
    const tokens = ['stock', 'scheme', 'amc', 'period', 'activityType', 'mcap', 'category', 'subCategory', 'view'];
    tokens.forEach(k => { if (filters[k]) parts.push(slugify(filters[k])); });
    const now = new Date();
    parts.push(`${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, '0')}${String(now.getDate()).padStart(2, '0')}`);
    return parts.join('-') + '.' + ext;
}

function slugify(str) {
    return String(str || '')
        .toLowerCase()
        .replace(/[^\w\s-]/g, '')
        .replace(/[\s_]+/g, '-')
        .replace(/^-+|-+$/g, '')
        .slice(0, 30);
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. buildMetadataBlock
// ─────────────────────────────────────────────────────────────────────────────

export function buildMetadataBlock({ title, filters = {}, rowCount }) {
    const now = new Date();
    const dateStr = now.toLocaleDateString('en-IN', { day: '2-digit', month: 'short', year: 'numeric' });
    const lines = [
        { label: 'Report', value: title },
        { label: 'Generated', value: dateStr },
        { label: 'Source', value: 'AMC Monthly Portfolio Disclosures (SEBI)' },
    ];
    const entries = Object.entries(filters).filter(([, v]) => v && v !== 'All' && v !== '');
    if (entries.length) {
        lines.push({ label: '', value: '' });
        entries.forEach(([k, v]) => lines.push({ label: formatLabel(k), value: String(v) }));
    }
    lines.push({ label: '', value: '' });
    lines.push({ label: 'Rows Exported', value: String(rowCount) });
    lines.push({ label: '', value: '' });
    return lines;
}

function formatLabel(key) {
    // Already human-readable: has spaces, or is all-caps acronym (AMC, ISIN, etc.)
    if (key.includes(' ') || key === key.toUpperCase()) return key;
    // camelCase → Title Case (e.g. planType → Plan Type)
    return key
        .replace(/([A-Z])/g, ' $1')
        .replace(/_/g, ' ')
        .replace(/^./, s => s.toUpperCase())
        .trim();
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. checkRowLimit
// ─────────────────────────────────────────────────────────────────────────────

export function checkRowLimit(data, format) {
    const limit = ROW_LIMITS[format] || 10000;
    if (data.length > limit) {
        return `Dataset has ${data.length.toLocaleString()} rows (limit: ${limit.toLocaleString()} for ${format.toUpperCase()}). Please apply filters to reduce the data.`;
    }
    return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: transform rows + detect num format for each column
// ─────────────────────────────────────────────────────────────────────────────

function transformRows(data, columns) {
    return data.map(row => {
        const out = {};
        columns.forEach(col => {
            const rawValue = typeof col.getValue === 'function' ? col.getValue(row) : row[col.key];
            out[col.label] = formatColumnValue(rawValue, col);
        });
        return out;
    });
}

/**
 * Detect the best number format code for a column label.
 */
function detectNumFmt(label) {
    const l = label.toLowerCase();
    if (l.includes('qty') || l.includes('shares') || l.includes('funds') || l.includes('schemes') || l.includes('change in')) return NUM_FMTS.integer;
    if (l.includes('%') || l.includes('pct') || l.includes('ownership') || l.includes('nav')) return NUM_FMTS.percent2;
    if (l.includes('ltp') || l.includes('price')) return NUM_FMTS.price;
    if (l.includes('aum') || l.includes('cr') || l.includes('cap') || l.includes('value')) return NUM_FMTS.decimal2;
    return NUM_FMTS.decimal2;
}

/**
 * Apply number formats to every numeric cell in an xlsx sheet to prevent scientific notation.
 */
function applyNumberFormats(sheet, columns) {
    try {
        const XLSX_utils = sheet.__xlsx_ref;
        if (!XLSX_utils) return; // will be patched after creation

        const range = XLSX_utils.decode_range(sheet['!ref'] || 'A1');
        for (let R = 1; R <= range.e.r; R++) { // skip header (R=0)
            columns.forEach((col, C) => {
                if (col.exportFormat !== 'numeric') return;
                const cellRef = XLSX_utils.encode_cell({ r: R, c: C });
                const cell = sheet[cellRef];
                if (!cell || cell.t !== 'n') return;
                cell.z = detectNumFmt(col.label);
            });
        }
    } catch (_) {/* non-critical */ }
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. exportToExcel — proper number formatting, multi-sheet
// ─────────────────────────────────────────────────────────────────────────────

export async function exportToExcel({ data, columns, fileName, metadata, extraSheets = [] }) {
    const limitError = checkRowLimit(data, 'excel');
    if (limitError) throw new Error(limitError);

    const XLSX = await import('xlsx');

    const workbook = XLSX.utils.book_new();

    // ── Sheet 1: Info ──
    const metaLines = buildMetadataBlock({ ...metadata, rowCount: data.length });
    const infoAoa = metaLines.map(({ label, value }) => [label, value]);
    const infoSheet = XLSX.utils.aoa_to_sheet(infoAoa);
    infoSheet['!cols'] = [{ wch: 24 }, { wch: 70 }];
    XLSX.utils.book_append_sheet(workbook, infoSheet, 'Info');

    // ── Sheet 2: Data (mirrors UI table) ──
    const sanitized = sanitizeData(data);
    const transformed = transformRows(sanitized, columns);
    const dataSheet = XLSX.utils.json_to_sheet(transformed);

    // Auto-fit column widths
    dataSheet['!cols'] = columns.map(col => {
        const maxLen = Math.max(
            col.label.length,
            ...sanitized.slice(0, 50).map(row => String(row[col.key] ?? '').length)
        );
        return { wch: Math.min(Math.max(maxLen + 2, 8), 40) };
    });

    // Freeze header row
    dataSheet['!freeze'] = { xSplit: 0, ySplit: 1, topLeftCell: 'A2', activePane: 'bottomLeft', state: 'frozen' };

    // ── Apply number formats to prevent scientific notation ──
    const range = XLSX.utils.decode_range(dataSheet['!ref'] || 'A1');
    for (let R = 1; R <= range.e.r; R++) { // R=0 is header
        columns.forEach((col, C) => {
            if (col.exportFormat !== 'numeric') return;
            const cellRef = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = dataSheet[cellRef];
            if (!cell || cell.t !== 'n') return;
            cell.z = detectNumFmt(col.label);
        });
    }

    XLSX.utils.book_append_sheet(workbook, dataSheet, 'Data');

    // ── Extra Sheets ──
    extraSheets.forEach(sheet => {
        if (!sheet.data?.length) return;
        const ws_sanitized = sanitizeData(sheet.data);
        const ws_transformed = transformRows(ws_sanitized, sheet.columns);
        const ws = XLSX.utils.json_to_sheet(ws_transformed);
        ws['!cols'] = sheet.columns.map(() => ({ wch: 18 }));

        // Apply number formats on extra sheets too
        const wsRange = XLSX.utils.decode_range(ws['!ref'] || 'A1');
        for (let R = 1; R <= wsRange.e.r; R++) {
            sheet.columns.forEach((col, C) => {
                if (col.exportFormat !== 'numeric') return;
                const cellRef = XLSX.utils.encode_cell({ r: R, c: C });
                const cell = ws[cellRef];
                if (!cell || cell.t !== 'n') return;
                cell.z = detectNumFmt(col.label);
            });
        }

        XLSX.utils.book_append_sheet(workbook, ws, sheet.name.slice(0, 31));
    });

    XLSX.writeFile(workbook, fileName);
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. exportToPDF — using autoTable(doc, {...}) function form
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Replace characters that jsPDF Helvetica cannot render.
 * ₹ → Rs.   and strip any formula-guard leading apostrophe.
 */
function pdfSafe(str) {
    if (str == null) return '';
    return String(str)
        .replace(/₹/g, 'Rs.')   // Rupee glyph not in Helvetica
        .replace(/\u20B9/g, 'Rs.')   // same via unicode escape
        .replace(/^'+/, '');    // strip leading formula-guard apostrophe
}

/** Format a numeric value for human-readable PDF display (no ₹ glyph) */
function formatForDisplay(value, colLabel) {
    if (value === '' || value === null || value === undefined) return '-';
    const l = String(colLabel).toLowerCase();
    const num = Number(value);
    if (isNaN(num)) return pdfSafe(String(value));

    if (l.includes('qty') || l.includes('shares') || l.includes('change in') || l.includes('chg')) {
        return num.toLocaleString('en-IN');
    }
    if (l.includes('%') || l.includes('own') || l.includes('ownership') || l.includes('nav') || l.includes('pct')) {
        return `${num.toFixed(2)}%`;
    }
    if (l.includes('aum') || l.includes('(cr)') || l.includes('cap')) {
        return `Rs.${num.toLocaleString('en-IN', { maximumFractionDigits: 2 })}`;
    }
    if (l.includes('ltp') || l.includes('price')) {
        return `Rs.${num.toLocaleString('en-IN', { maximumFractionDigits: 2 })}`;
    }
    return num.toLocaleString('en-IN', { maximumFractionDigits: 4 });
}

export async function exportToPDF({ data, columns, fileName, title, metadata }) {
    const limitError = checkRowLimit(data, 'pdf');
    if (limitError) throw new Error(limitError);

    const { default: jsPDF } = await import('jspdf');
    const { default: autoTable } = await import('jspdf-autotable');

    const doc = new jsPDF({ orientation: 'landscape', unit: 'mm', format: 'a4' });
    const pageWidth = doc.internal.pageSize.getWidth();

    // ── Header ──
    doc.setFontSize(14);
    doc.setTextColor(24, 31, 60);
    doc.setFont('helvetica', 'bold');
    doc.text('MF Holdings Report', 14, 14);

    doc.setFontSize(9);
    doc.setFont('helvetica', 'normal');
    doc.setTextColor(80, 80, 100);
    doc.text(pdfSafe(title), 14, 21);

    // ── Metadata ── (run pdfSafe on every value so ₹ doesn't break rendering)
    const metaLines = buildMetadataBlock({ ...metadata, rowCount: data.length });
    doc.setFontSize(7.5);
    doc.setTextColor(100, 100, 120);
    let yPos = 27;
    metaLines.forEach(({ label, value }) => {
        if (!label && !value) { yPos += 2; return; }
        const line = label
            ? `${pdfSafe(label)}: ${pdfSafe(value)}`
            : pdfSafe(value);
        doc.text(line, 14, yPos);
        yPos += 3.5;
    });

    doc.setDrawColor(200, 200, 220);
    doc.line(14, yPos, pageWidth - 14, yPos);
    yPos += 3;

    // ── Table ──
    const headers = columns.map(col => pdfSafe(col.label));
    const sanitized = sanitizeData(data);
    const rows = sanitized.map(row =>
        columns.map(col => {
            const rawValue = typeof col.getValue === 'function' ? col.getValue(row) : row[col.key];
            if (col.exportFormat === 'numeric') return formatForDisplay(rawValue, col.label);
            const s = rawValue == null || rawValue === '' ? '-' : String(rawValue);
            return pdfSafe(s);
        })
    );

    // Column width strategy: fill the full page, give text cols a fixed budget,
    // distribute remaining width evenly across numeric cols.
    // With 4 months (8 numeric) + 2 text cols on A4 landscape (277mm usable):
    //   text = 2 × 32mm = 64mm, numeric = (277-64)/8 = 26.6mm each — perfect.
    const availableWidth = pageWidth - 20; // 10mm margin each side
    const stringCols = columns.filter(c => c.exportFormat === 'string');
    const numericCols = columns.filter(c => c.exportFormat !== 'string');

    // Fixed per-column budget for text: 32mm each (min 20mm), cap total at 50% of page
    const textPerCol = Math.max(20, Math.min(35, (availableWidth * 0.5) / Math.max(stringCols.length, 1)));
    const totalTextWidth = stringCols.length * textPerCol;
    const numPerCol = numericCols.length > 0
        ? Math.max(16, (availableWidth - totalTextWidth) / numericCols.length)
        : 0;

    const colStyles = {};
    columns.forEach((col, i) => {
        if (col.exportFormat === 'string') {
            colStyles[i] = { cellWidth: textPerCol };
        } else {
            colStyles[i] = { cellWidth: numPerCol, halign: 'right' };
        }
    });

    autoTable(doc, {
        head: [headers],
        body: rows,
        startY: yPos,
        margin: { left: 10, right: 10 },
        tableWidth: availableWidth,
        styles: {
            fontSize: 6.5,
            cellPadding: { top: 1.5, right: 2, bottom: 1.5, left: 2 },
            overflow: 'linebreak',
            textColor: [30, 35, 60],
            minCellHeight: 6,
        },
        headStyles: {
            fillColor: [26, 31, 54],
            textColor: [255, 255, 255],
            fontStyle: 'bold',
            fontSize: 6.5,
            halign: 'center',
            overflow: 'linebreak',
        },
        alternateRowStyles: { fillColor: [245, 246, 252] },
        columnStyles: colStyles,
    });

    // ── Footer ──
    const pageCount = doc.internal.getNumberOfPages();
    for (let i = 1; i <= pageCount; i++) {
        doc.setPage(i);
        doc.setFontSize(6.5);
        doc.setTextColor(150);
        doc.text(
            `Data: AMC Monthly Portfolio Disclosures (SEBI) | Page ${i}/${pageCount}`,
            10,
            doc.internal.pageSize.getHeight() - 6
        );
        doc.text(
            new Date().toLocaleDateString('en-IN'),
            pageWidth - 10,
            doc.internal.pageSize.getHeight() - 6,
            { align: 'right' }
        );
    }

    doc.save(fileName);
}

// ─────────────────────────────────────────────────────────────────────────────
// 8. exportToCSV — with metadata block at top
// ─────────────────────────────────────────────────────────────────────────────

export function exportToCSV({ data, columns, fileName, metadata }) {
    const limitError = checkRowLimit(data, 'csv');
    if (limitError) throw new Error(limitError);

    const sanitized = sanitizeData(data);
    const lines = [];

    // Metadata header
    if (metadata) {
        const metaLines = buildMetadataBlock({ ...metadata, rowCount: data.length });
        metaLines.forEach(({ label, value }) => {
            if (!label && !value) { lines.push(''); return; }
            lines.push(`"${label}","${String(value).replace(/"/g, '""')}"`);
        });
        lines.push('');
        lines.push('"--- DATA ---"');
        lines.push('');
    }

    // Header row
    lines.push(columns.map(col => `"${col.label}"`).join(','));

    // Data rows — use raw numbers for analyst pipelines
    sanitized.forEach(row => {
        const cells = columns.map(col => {
            const raw = typeof col.getValue === 'function' ? col.getValue(row) : row[col.key];
            if (col.exportFormat === 'numeric') {
                const n = Number(raw);
                return isNaN(n) ? '' : n;
            }
            const s = raw == null ? '' : String(raw);
            return `"${s.replace(/"/g, '""')}"`;
        });
        lines.push(cells.join(','));
    });

    const blob = new Blob(['\uFEFF' + lines.join('\r\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}
