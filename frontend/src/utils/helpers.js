/**
 * Debounce function to limit API calls
 */
export function debounce(func, wait) {
    let timeout;

    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };

        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Format number with Indian numbering system (lakhs, crores)
 */
export function formatIndianNumber(num) {
    if (num === null || num === undefined) return '-';

    // Convert to number if it's a string
    const numValue = Number(num);
    if (isNaN(numValue)) return '-';

    const absNum = Math.abs(numValue);

    if (absNum >= 10000000) {
        return `₹${(numValue / 10000000).toFixed(2)} Cr`;
    } else if (absNum >= 100000) {
        return `₹${(numValue / 100000).toFixed(2)} L`;
    } else if (absNum >= 1000) {
        return `₹${(numValue / 1000).toFixed(2)} K`;
    }

    return `₹${numValue.toFixed(2)}`;
}

/**
 * Format number in Crores with fixed pattern (e.g., ₹1.00 Cr)
 */
export function formatCrores(num) {
    if (num === null || num === undefined) return '-';

    // API values are usually in Crores, but if passed in absolute Rupees:
    // This function assumes the input is the absolute Rupee value
    const numValue = Number(num);
    if (isNaN(numValue)) return '-';

    return `₹${(numValue / 10000000).toLocaleString('en-IN', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    })} Cr`;
}

/**
 * Format percentage
 */
export function formatPercent(num, decimals = 2) {
    if (num === null || num === undefined) return '-';

    // Convert to number if it's a string
    const numValue = Number(num);
    if (isNaN(numValue)) return '-';

    return `${numValue.toFixed(decimals)}%`;
}

/**
 * Format large numbers with commas
 */
export function formatNumber(num) {
    if (num === null || num === undefined) return '-';

    // Convert to number if it's a string
    const numValue = Number(num);
    if (isNaN(numValue)) return '-';

    return numValue.toLocaleString('en-IN');
}
