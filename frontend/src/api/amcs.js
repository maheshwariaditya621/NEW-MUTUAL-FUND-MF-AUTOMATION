import { apiGet } from './client';

export async function listAMCs() {
    return apiGet('/amcs');
}

export async function getAMCDetail(amcId) {
    return apiGet(`/amcs/${amcId}`);
}

export async function searchAMCs(query) {
    return apiGet('/amcs/search', { q: query });
}
