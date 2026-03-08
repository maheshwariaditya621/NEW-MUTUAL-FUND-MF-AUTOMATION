export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_URL || '/api/v1';

/**
 * Base HTTP client for API requests
 */
export async function apiGet(endpoint, params = {}) {
    const url = new URL(`${API_BASE_URL}${endpoint}`, window.location.origin);

    // Add query parameters
    Object.keys(params).forEach(key => {
        if (params[key] !== undefined && params[key] !== null) {
            url.searchParams.append(key, params[key]);
        }
    });

    const token = localStorage.getItem('token');

    try {
        const response = await fetch(url.toString(), {
            headers: {
                ...(token ? { 'Authorization': `Bearer ${token}` } : {})
            }
        });

        if (!response.ok) {
            if (response.status === 401 && !url.toString().includes('/auth/login')) {
                window.dispatchEvent(new CustomEvent('api-unauthorized'));
            }
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP Error: ${response.status} ${response.statusText}`);
        }

        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

/**
 * Base HTTP client for API POST requests
 */
export async function apiPost(endpoint, data = {}) {
    const url = `${API_BASE_URL}${endpoint}`;
    const token = localStorage.getItem('token');

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...(token ? { 'Authorization': `Bearer ${token}` } : {})
            },
            body: JSON.stringify(data),
        });

        if (!response.ok) {
            if (response.status === 401 && !url.includes('/auth/login')) {
                window.dispatchEvent(new CustomEvent('api-unauthorized'));
            }
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP Error: ${response.status} ${response.statusText}`);
        }

        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

/**
 * HTTP client for OAuth2 Form POST requests (Login)
 */
export async function apiPostForm(endpoint, formData) {
    const url = `${API_BASE_URL}${endpoint}`;

    try {
        const response = await fetch(url, {
            method: 'POST',
            // Fetch will automatically set content-type for URLSearchParams
            body: formData,
        });

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.detail || `HTTP Error: ${response.status} ${response.statusText}`);
        }

        return await response.json();
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

/**
 * Generic error handler for API calls
 */
export function handleApiError(error) {
    if (error.message) {
        return error.message;
    }
    return 'An unexpected error occurred. Please try again.';
}
