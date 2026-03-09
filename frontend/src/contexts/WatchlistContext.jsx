import React, { createContext, useState, useContext, useEffect, useCallback } from 'react';
import { useAuth } from './AuthContext';
import {
    getWatchlistItems,
    addWatchlistItem,
    removeWatchlistItem,
    getPreferences,
    updatePreferences as apiUpdatePreferences,
} from '../api/watchlist';

const WatchlistContext = createContext(null);

export const WatchlistProvider = ({ children }) => {
    const { isAuthenticated } = useAuth();

    // Map of "stock-{isin}" or "scheme-{schemeId}" -> watchlist_id
    const [watchedMap, setWatchedMap] = useState({});
    const [preferences, setPreferences] = useState({
        mf_buying: true,
        mf_selling: true,
        net_activity: true,
        top_holders: false,
        trend_indicator: false,
        popularity_score: false,
    });
    const [loading, setLoading] = useState(false);
    // Increments every time an item is added or removed — lets consumers re-fetch
    const [version, setVersion] = useState(0);
    const bumpVersion = useCallback(() => setVersion(v => v + 1), []);

    // ── Load watchlist + preferences on login ────────────────────────────────
    useEffect(() => {
        if (!isAuthenticated) {
            setWatchedMap({});
            setPreferences({
                mf_buying: true, mf_selling: true, net_activity: true,
                top_holders: false, trend_indicator: false, popularity_score: false,
            });
            return;
        }

        const loadData = async () => {
            setLoading(true);
            try {
                const [itemsData, prefs] = await Promise.all([
                    getWatchlistItems(),
                    getPreferences(),
                ]);

                const map = {};
                (itemsData?.items || []).forEach(item => {
                    const key = item.asset_type === 'stock'
                        ? `stock-${item.isin}`
                        : `scheme-${item.scheme_id}`;
                    map[key] = item.watchlist_id;
                });
                setWatchedMap(map);
                setPreferences(prev => ({ ...prev, ...prefs }));
            } catch (e) {
                console.error('Failed to load watchlist:', e);
            } finally {
                setLoading(false);
            }
        };

        loadData();
    }, [isAuthenticated]);

    // ── Helpers ──────────────────────────────────────────────────────────────
    const isWatched = useCallback((assetType, identifier) => {
        const key = `${assetType}-${identifier}`;
        return !!watchedMap[key];
    }, [watchedMap]);

    const getWatchlistId = useCallback((assetType, identifier) => {
        return watchedMap[`${assetType}-${identifier}`] || null;
    }, [watchedMap]);

    const addToWatchlist = useCallback(async ({ assetType, isin, schemeId, name }) => {
        const key = assetType === 'stock' ? `stock-${isin}` : `scheme-${schemeId}`;

        // Optimistic update
        setWatchedMap(prev => ({ ...prev, [key]: 'pending' }));

        try {
            const result = await addWatchlistItem({
                asset_type: assetType,
                isin: assetType === 'stock' ? isin : undefined,
                scheme_id: assetType === 'scheme' ? schemeId : undefined,
            });
            setWatchedMap(prev => ({ ...prev, [key]: result.watchlist_id }));
            bumpVersion(); // ← notify dashboard to reload
            return result;
        } catch (e) {
            // Revert on failure
            setWatchedMap(prev => {
                const next = { ...prev };
                delete next[key];
                return next;
            });
            console.error('Failed to add to watchlist:', e);
            throw e;
        }
    }, [bumpVersion]);

    const removeFromWatchlist = useCallback(async (assetType, identifier) => {
        const key = `${assetType}-${identifier}`;
        const watchlistId = watchedMap[key];
        if (!watchlistId) return;

        // Optimistic update
        setWatchedMap(prev => {
            const next = { ...prev };
            delete next[key];
            return next;
        });

        try {
            await removeWatchlistItem(watchlistId);
            bumpVersion(); // ← notify dashboard to reload
        } catch (e) {
            // Revert on failure
            setWatchedMap(prev => ({ ...prev, [key]: watchlistId }));
            console.error('Failed to remove from watchlist:', e);
            throw e;
        }
    }, [watchedMap, bumpVersion]);

    const updatePreferences = useCallback(async (newPrefs) => {
        const merged = { ...preferences, ...newPrefs };
        setPreferences(merged); // optimistic
        try {
            await apiUpdatePreferences(merged);
        } catch (e) {
            setPreferences(preferences); // revert
            console.error('Failed to save preferences:', e);
        }
    }, [preferences]);

    const refreshWatchlist = useCallback(async () => {
        try {
            const data = await getWatchlistItems();
            const map = {};
            (data?.items || []).forEach(item => {
                const key = item.asset_type === 'stock'
                    ? `stock-${item.isin}`
                    : `scheme-${item.scheme_id}`;
                map[key] = item.watchlist_id;
            });
            setWatchedMap(map);
        } catch (e) {
            console.error('Failed to refresh watchlist:', e);
        }
    }, []);

    return (
        <WatchlistContext.Provider value={{
            watchedMap,
            preferences,
            loading,
            version,
            isWatched,
            getWatchlistId,
            addToWatchlist,
            removeFromWatchlist,
            updatePreferences,
            refreshWatchlist,
        }}>
            {children}
        </WatchlistContext.Provider>
    );
};

export const useWatchlist = () => {
    const ctx = useContext(WatchlistContext);
    if (!ctx) throw new Error('useWatchlist must be used within WatchlistProvider');
    return ctx;
};
