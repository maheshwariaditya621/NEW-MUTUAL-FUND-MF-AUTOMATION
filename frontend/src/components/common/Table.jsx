import { useState } from 'react';
import { ArrowUp, ArrowDown } from 'lucide-react';
import './Table.css';

export default function Table({ columns, data, defaultSort = null }) {
    const [sortConfig, setSortConfig] = useState(defaultSort);

    const handleSort = (key) => {
        const column = columns.find(col => col.key === key);
        if (!column?.sortable) return;

        let direction = 'asc';
        if (sortConfig?.key === key && sortConfig.direction === 'asc') {
            direction = 'desc';
        }

        setSortConfig({ key, direction });
    };

    const sortedData = [...data].sort((a, b) => {
        if (!sortConfig) return 0;

        const column = columns.find(col => col.key === sortConfig.key);
        const aValue = column?.sortValue ? column.sortValue(a) : a[sortConfig.key];
        const bValue = column?.sortValue ? column.sortValue(b) : b[sortConfig.key];

        if (aValue === null || aValue === undefined) return 1;
        if (bValue === null || bValue === undefined) return -1;

        if (typeof aValue === 'number' && typeof bValue === 'number') {
            return sortConfig.direction === 'asc' ? aValue - bValue : bValue - aValue;
        }

        const aStr = String(aValue).toLowerCase();
        const bStr = String(bValue).toLowerCase();

        if (sortConfig.direction === 'asc') {
            return aStr.localeCompare(bStr);
        } else {
            return bStr.localeCompare(aStr);
        }
    });

    return (
        <div className="table-container">
            <table className="table">
                <thead>
                    <tr>
                        {columns.map((column) => (
                            <th
                                key={column.key}
                                className={column.sortable ? 'sortable' : ''}
                                onClick={() => handleSort(column.key)}
                            >
                                <div className="th-content">
                                    <span>{column.label}</span>
                                    {column.sortable && sortConfig?.key === column.key && (
                                        <span className="sort-icon">
                                            {sortConfig.direction === 'asc' ? (
                                                <ArrowUp size={16} />
                                            ) : (
                                                <ArrowDown size={16} />
                                            )}
                                        </span>
                                    )}
                                </div>
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {sortedData.map((row, rowIndex) => (
                        <tr key={rowIndex}>
                            {columns.map((column) => (
                                <td key={column.key} className={column.className || ''}>
                                    {column.render
                                        ? column.render(row[column.key], row)
                                        : row[column.key]}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>

            {data.length === 0 && (
                <div className="table-empty">
                    <p>No data available</p>
                </div>
            )}
        </div>
    );
}
