import { useState, useRef, useEffect } from 'react';
import { Download, FileSpreadsheet, FileText, Table2, ChevronDown, Loader2 } from 'lucide-react';
import {
    exportToExcel,
    exportToPDF,
    exportToCSV,
    generateFileName,
    checkRowLimit,
} from '../../utils/exportUtils';
import './ExportButton.css';

/**
 * ExportButton — Premium, reusable export dropdown component.
 *
 * Props:
 * @param {Function} getData             — Function returning the current visible rows (already filtered/sorted)
 * @param {Array}    columns             — Column configs: [{ key, label, exportFormat, formatter?, getValue? }]
 * @param {Array}    [pdfColumns]        — Subset column configs for PDF (to keep PDF readable)
 * @param {string}   [fileName]          — Base filename (extension added automatically)
 * @param {{ page: string, filters?: Object }} [fileNameConfig] — Used by generateFileName if fileName not provided
 * @param {{ title: string, filters?: Object }} metadata — Context for the metadata block
 * @param {Array}    [extraSheets]       — Extra Excel sheets: [{ name, data, columns }]
 */
export default function ExportButton({
    getData,
    columns,
    pdfColumns,
    fileName,
    fileNameConfig,
    metadata,
    extraSheets = [],
}) {
    const [isOpen, setIsOpen] = useState(false);
    const [isExporting, setIsExporting] = useState(false);
    const [exportError, setExportError] = useState(null);
    const dropdownRef = useRef(null);

    // Close dropdown when clicking outside
    useEffect(() => {
        const handler = (e) => {
            if (dropdownRef.current && !dropdownRef.current.contains(e.target)) {
                setIsOpen(false);
            }
        };
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, []);

    const getRowCount = () => {
        try {
            const d = getData();
            return Array.isArray(d) ? d.length : 0;
        } catch {
            return 0;
        }
    };

    const resolveFileName = (ext) => {
        if (fileName) return `${fileName}.${ext}`;
        if (fileNameConfig) return generateFileName({ ...fileNameConfig, ext });
        return `rupeevest-export-${Date.now()}.${ext}`;
    };

    const handleExport = async (format) => {
        if (isExporting) return;

        setIsExporting(true);
        setExportError(null);
        setIsOpen(false);

        try {
            const data = getData();

            // Row limit check
            const limitError = checkRowLimit(data, format);
            if (limitError) {
                setExportError(limitError);
                return;
            }

            if (format === 'excel') {
                await exportToExcel({
                    data,
                    columns,
                    fileName: resolveFileName('xlsx'),
                    metadata,
                    extraSheets,
                });
            } else if (format === 'pdf') {
                await exportToPDF({
                    data,
                    columns: pdfColumns || columns,
                    fileName: resolveFileName('pdf'),
                    title: metadata?.title || 'Rupeevest Report',
                    metadata,
                });
            } else if (format === 'csv') {
                exportToCSV({
                    data,
                    columns,
                    fileName: resolveFileName('csv'),
                    metadata,
                });
            }
        } catch (err) {
            setExportError(err.message || 'Export failed. Please try again.');
            console.error('[ExportButton] Export error:', err);
        } finally {
            setIsExporting(false);
        }
    };

    const rowCount = getRowCount();

    return (
        <div className="eb-wrapper" ref={dropdownRef}>
            {/* Trigger Button */}
            <button
                className={`eb-trigger ${isOpen ? 'eb-trigger--open' : ''} ${isExporting ? 'eb-trigger--loading' : ''}`}
                onClick={() => !isExporting && setIsOpen(prev => !prev)}
                disabled={isExporting || rowCount === 0}
                title={rowCount === 0 ? 'No data to export' : 'Export data'}
            >
                {isExporting ? (
                    <>
                        <Loader2 size={14} className="eb-spinner" />
                        <span>Exporting...</span>
                    </>
                ) : (
                    <>
                        <Download size={14} />
                        <span>Export</span>
                        <ChevronDown size={12} className={`eb-chevron ${isOpen ? 'eb-chevron--open' : ''}`} />
                    </>
                )}
            </button>

            {/* Dropdown */}
            {isOpen && (
                <div className="eb-dropdown">
                    <div className="eb-dropdown-header">
                        Export {rowCount.toLocaleString()} rows
                    </div>

                    <button className="eb-option" onClick={() => handleExport('excel')}>
                        <FileSpreadsheet size={15} className="eb-option-icon eb-option-icon--excel" />
                        <div className="eb-option-text">
                            <span className="eb-option-label">Download Excel</span>
                            <span className="eb-option-sub">.xlsx · Multi-sheet · Bold headers</span>
                        </div>
                        <span className="eb-option-count">{rowCount.toLocaleString()}</span>
                    </button>

                    <button className="eb-option" onClick={() => handleExport('pdf')}>
                        <FileText size={15} className="eb-option-icon eb-option-icon--pdf" />
                        <div className="eb-option-text">
                            <span className="eb-option-label">Download PDF</span>
                            <span className="eb-option-sub">.pdf · Professional layout</span>
                        </div>
                        <span className="eb-option-count">{Math.min(rowCount, 5000).toLocaleString()}</span>
                    </button>

                    <button className="eb-option" onClick={() => handleExport('csv')}>
                        <Table2 size={15} className="eb-option-icon eb-option-icon--csv" />
                        <div className="eb-option-text">
                            <span className="eb-option-label">Download CSV</span>
                            <span className="eb-option-sub">.csv · For Python / Excel pipelines</span>
                        </div>
                        <span className="eb-option-count">{rowCount.toLocaleString()}</span>
                    </button>
                </div>
            )}

            {/* Error Toast */}
            {exportError && (
                <div className="eb-error" onClick={() => setExportError(null)}>
                    <span>⚠ {exportError}</span>
                    <span className="eb-error-dismiss">✕</span>
                </div>
            )}
        </div>
    );
}
