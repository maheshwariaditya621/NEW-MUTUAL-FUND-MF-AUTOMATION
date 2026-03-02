import React, { useState, useEffect } from 'react';
import {
    Trash2, Download, Database, FileText,
    Search, RefreshCw, HardDrive, AlertTriangle,
    ChevronDown, ChevronUp, CheckCircle, XCircle
} from 'lucide-react';

const FileManagement = () => {
    const [inventory, setInventory] = useState([]);
    const [stats, setStats] = useState({ raw_size_mb: 0, merged_size_mb: 0, total_size_mb: 0 });
    const [loading, setLoading] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');
    const [filterYear, setFilterYear] = useState('All');
    const [filterMonth, setFilterMonth] = useState('All');
    const [error, setError] = useState(null);
    const [actionLoading, setActionLoading] = useState(null);

    useEffect(() => {
        fetchInventory();
    }, []);

    const fetchInventory = async () => {
        setLoading(true);
        try {
            const response = await fetch('/api/admin/files/inventory');
            const data = await response.json();
            if (data.status === 'success') {
                setInventory(data.inventory);
                setStats(data.stats);
            } else {
                setError(data.detail || 'Failed to load inventory');
            }
        } catch (err) {
            setError('Connection error while fetching inventory');
        } finally {
            setLoading(false);
        }
    };

    const handleDelete = async (slug, year, month, category) => {
        if (!window.confirm(`Are you sure you want to delete the ${category} data for ${slug.toUpperCase()} ${month}/${year}?`)) {
            return;
        }

        const actionKey = `${slug}-${year}-${month}-${category}`;
        setActionLoading(actionKey);

        try {
            const response = await fetch(`/api/admin/files?amc_slug=${slug}&year=${year}&month=${month}&category=${category}`, {
                method: 'DELETE'
            });
            const data = await response.json();

            if (data.status === 'success') {
                fetchInventory(); // Refresh after success
            } else {
                alert(data.detail || 'Failed to delete file');
            }
        } catch (err) {
            alert('Error during deletion');
        } finally {
            setActionLoading(null);
        }
    };

    const handleBulkDelete = async (year, month, category) => {
        if (!window.confirm(`🚨 CRITICAL: Are you sure you want to delete ALL ${category} files for the entire month of ${month}/${year}?`)) {
            return;
        }

        setActionLoading(`bulk-${year}-${month}-${category}`);

        try {
            const response = await fetch(`/api/admin/files/bulk?year=${year}&month=${month}&category=${category}`, {
                method: 'DELETE'
            });
            const data = await response.json();

            if (data.status === 'success') {
                alert(data.message);
                fetchInventory();
            } else {
                alert(data.detail || 'Failed bulk delete');
            }
        } catch (err) {
            alert('Error during bulk deletion');
        } finally {
            setActionLoading(null);
        }
    };

    // Filter logic
    const filteredInventory = inventory.filter(item => {
        const matchesSearch = item.amc_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            item.amc_slug.toLowerCase().includes(searchTerm.toLowerCase());
        const matchesYear = filterYear === 'All' || item.year.toString() === filterYear;
        const matchesMonth = filterMonth === 'All' || item.month.toString() === filterMonth;
        return matchesSearch && matchesYear && matchesMonth;
    });

    const years = ['All', ...new Set(inventory.map(i => i.year.toString()))].sort().reverse();
    const months = ['All', ...Array.from({ length: 12 }, (_, i) => i + 1)];

    return (
        <div className="p-6 max-w-7xl mx-auto bg-gray-50 min-h-screen">
            <div className="flex justify-between items-center mb-8">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900 mb-2">Admin File Management</h1>
                    <p className="text-gray-500">Manage raw downloads and consolidated Excel files directly from the server.</p>
                </div>
                <button
                    onClick={fetchInventory}
                    className="p-2 bg-white rounded-lg shadow-sm border hover:bg-gray-50 flex items-center gap-2"
                >
                    <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
                    Refresh
                </button>
            </div>

            {/* Stats Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                <div className="bg-white p-6 rounded-xl shadow-sm border flex items-center gap-4">
                    <div className="p-3 bg-blue-100 text-blue-600 rounded-lg">
                        <HardDrive className="w-6 h-6" />
                    </div>
                    <div>
                        <p className="text-sm font-medium text-gray-500 uppercase">Total Storage</p>
                        <p className="text-2xl font-bold text-gray-900">{stats.total_size_mb} MB</p>
                    </div>
                </div>
                <div className="bg-white p-6 rounded-xl shadow-sm border flex items-center gap-4">
                    <div className="p-3 bg-indigo-100 text-indigo-600 rounded-lg">
                        <FileText className="w-6 h-6" />
                    </div>
                    <div>
                        <p className="text-sm font-medium text-gray-500 uppercase">Raw Data</p>
                        <p className="text-2xl font-bold text-gray-900">{stats.raw_size_mb} MB</p>
                    </div>
                </div>
                <div className="bg-white p-6 rounded-xl shadow-sm border flex items-center gap-4">
                    <div className="p-3 bg-emerald-100 text-emerald-600 rounded-lg">
                        <CheckCircle className="w-6 h-6" />
                    </div>
                    <div>
                        <p className="text-sm font-medium text-gray-500 uppercase">Merged Files</p>
                        <p className="text-2xl font-bold text-gray-900">{stats.merged_size_mb} MB</p>
                    </div>
                </div>
            </div>

            {/* Filters */}
            <div className="bg-white p-4 rounded-xl shadow-sm border mb-6 flex flex-wrap gap-4 items-end">
                <div className="flex-1 min-w-[200px]">
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-1">Search AMC</label>
                    <div className="relative">
                        <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
                        <input
                            type="text"
                            placeholder="HDFC, Nippon, etc..."
                            className="w-full pl-10 pr-4 py-2 bg-gray-50 border rounded-lg focus:ring-2 focus:ring-blue-500 outline-none"
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                    </div>
                </div>
                <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-1">Year</label>
                    <select
                        className="px-4 py-2 bg-gray-50 border rounded-lg outline-none"
                        value={filterYear}
                        onChange={(e) => setFilterYear(e.target.value)}
                    >
                        {years.map(y => <option key={y} value={y}>{y}</option>)}
                    </select>
                </div>
                <div>
                    <label className="block text-xs font-semibold text-gray-500 uppercase mb-1">Month</label>
                    <select
                        className="px-4 py-2 bg-gray-50 border rounded-lg outline-none"
                        value={filterMonth}
                        onChange={(e) => setFilterMonth(e.target.value)}
                    >
                        {months.map(m => <option key={m} value={m}>{m}</option>)}
                    </select>
                </div>
            </div>

            {/* Inventory Table */}
            <div className="bg-white rounded-xl shadow-sm border overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full text-left">
                        <thead className="bg-gray-50 border-b">
                            <tr>
                                <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase">AMC Name</th>
                                <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase text-center">Period</th>
                                <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase text-center">Raw Data</th>
                                <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase text-center">Merged Excel</th>
                                <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase text-center">DB Loaded</th>
                                <th className="px-6 py-4 text-xs font-bold text-gray-500 uppercase text-right">Actions</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y">
                            {filteredInventory.map((item, idx) => (
                                <tr key={`${item.amc_slug}-${item.year}-${item.month}`} className="hover:bg-blue-50/30 transition-colors">
                                    <td className="px-6 py-4">
                                        <div className="font-bold text-gray-900">{item.amc_name}</div>
                                        <div className="text-xs text-gray-400 font-mono uppercase">{item.amc_slug}</div>
                                    </td>
                                    <td className="px-6 py-4 text-center">
                                        <span className="px-2 py-1 bg-gray-100 rounded text-sm font-medium text-gray-700">
                                            {item.month}/{item.year}
                                        </span>
                                    </td>
                                    <td className="px-6 py-4 text-center">
                                        {item.raw_present ? (
                                            <div className="flex flex-col items-center gap-1">
                                                <CheckCircle className="w-5 h-5 text-emerald-500" />
                                                <span className="text-[10px] font-bold text-emerald-600 uppercase">Present</span>
                                            </div>
                                        ) : (
                                            <XCircle className="w-5 h-5 text-gray-200 mx-auto" />
                                        )}
                                    </td>
                                    <td className="px-6 py-4 text-center">
                                        {item.merged_present ? (
                                            <div className="flex flex-col items-center gap-1">
                                                <CheckCircle className="w-5 h-5 text-emerald-500" />
                                                <span className="text-[10px] font-bold text-emerald-600 uppercase">Present</span>
                                            </div>
                                        ) : (
                                            <XCircle className="w-5 h-5 text-gray-200 mx-auto" />
                                        )}
                                    </td>
                                    <td className="px-6 py-4 text-center">
                                        {item.db_loaded ? (
                                            <div className="inline-flex items-center gap-1 px-2 py-1 bg-indigo-50 text-indigo-700 rounded-full text-[10px] font-bold uppercase ring-1 ring-indigo-200">
                                                <Database className="w-3 h-3" />
                                                {item.snapshots_count} Snapshots
                                            </div>
                                        ) : (
                                            <span className="text-[10px] font-bold text-gray-300 uppercase">No Data</span>
                                        )}
                                    </td>
                                    <td className="px-6 py-4 text-right">
                                        <div className="flex justify-end gap-2">
                                            <button
                                                disabled={!item.raw_present || actionLoading === `${item.amc_slug}-${item.year}-${item.month}-raw`}
                                                onClick={() => handleDelete(item.amc_slug, item.year, item.month, 'raw')}
                                                className="p-1.5 text-gray-400 hover:text-red-500 disabled:opacity-30 disabled:hover:text-gray-400"
                                                title="Delete Raw Folder"
                                            >
                                                <Trash2 className="w-4 h-4" />
                                            </button>
                                            <button
                                                disabled={!item.merged_present || actionLoading === `${item.amc_slug}-${item.year}-${item.month}-merged`}
                                                onClick={() => handleDelete(item.amc_slug, item.year, item.month, 'merged')}
                                                className="p-1.5 text-gray-400 hover:text-red-500 disabled:opacity-30 disabled:hover:text-gray-400"
                                                title="Delete Merged Excel"
                                            >
                                                <Trash2 className="w-4 h-4" />
                                            </button>
                                        </div>
                                    </td>
                                </tr>
                            ))}
                            {filteredInventory.length === 0 && !loading && (
                                <tr>
                                    <td colSpan="6" className="px-6 py-12 text-center text-gray-400 italic">
                                        No matching files found.
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                </div>
            </div>

            {/* Bulk Options (Footer) */}
            <div className="mt-8 bg-red-50 border border-red-100 rounded-xl p-6">
                <div className="flex items-start gap-4">
                    <AlertTriangle className="w-6 h-6 text-red-600 mt-1" />
                    <div>
                        <h3 className="text-lg font-bold text-red-900 mb-1">Bulk Cleanup Actions</h3>
                        <p className="text-sm text-red-700 mb-4">Wipe entire months of files for all AMCs. This action is irreversible.</p>

                        <div className="flex flex-wrap gap-4">
                            {/* Year/Month selectors for bulk */}
                            <div className="flex gap-2">
                                <select id="bulk-year" className="px-3 py-1.5 bg-white border rounded text-sm">
                                    {years.filter(y => y !== 'All').map(y => <option key={y} value={y}>{y}</option>)}
                                </select>
                                <select id="bulk-month" className="px-3 py-1.5 bg-white border rounded text-sm">
                                    {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map(m => <option key={m} value={m}>{m}</option>)}
                                </select>
                            </div>

                            <div className="flex gap-2">
                                <button
                                    onClick={() => {
                                        const y = document.getElementById('bulk-year').value;
                                        const m = document.getElementById('bulk-month').value;
                                        handleBulkDelete(y, m, 'raw');
                                    }}
                                    className="px-4 py-2 bg-red-600 text-white text-sm font-bold rounded-lg hover:bg-red-700 transition-colors"
                                >
                                    Wipe All Raw For Selected Month
                                </button>
                                <button
                                    onClick={() => {
                                        const y = document.getElementById('bulk-year').value;
                                        const m = document.getElementById('bulk-month').value;
                                        handleBulkDelete(y, m, 'merged');
                                    }}
                                    className="px-4 py-2 border border-red-600 text-red-600 text-sm font-bold rounded-lg hover:bg-red-50 transition-colors"
                                >
                                    Wipe All Merged For Selected Month
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default FileManagement;
