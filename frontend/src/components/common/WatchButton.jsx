import React, { useState } from 'react';
import { useWatchlist } from '../../contexts/WatchlistContext';
import './WatchButton.css';

/**
 * WatchButton — universal "+ Watch" / "✓ Watching" toggle button.
 *
 * Props:
 *   assetType  'stock' | 'scheme'
 *   isin       string  (required for stocks)
 *   schemeId   number  (required for schemes)
 *   name       string  display name for tooltip
 *   size       'sm' | 'md' (default 'md')
 *   variant    'filled' | 'outline' (default 'outline')
 */
const WatchButton = ({
    assetType,
    isin,
    schemeId,
    name = '',
    size = 'md',
    variant = 'outline',
}) => {
    const { isWatched, addToWatchlist, removeFromWatchlist } = useWatchlist();
    const [busy, setBusy] = useState(false);
    const [flash, setFlash] = useState(null); // 'added' | 'removed'

    const identifier = assetType === 'stock' ? isin : schemeId;
    const watched = isWatched(assetType, identifier);

    const handleClick = async (e) => {
        e.stopPropagation();
        if (busy) return;
        setBusy(true);
        try {
            if (watched) {
                await removeFromWatchlist(assetType, identifier);
                triggerFlash('removed');
            } else {
                await addToWatchlist({ assetType, isin, schemeId, name });
                triggerFlash('added');
            }
        } catch (_) { /* errors are logged in context */ }
        finally { setBusy(false); }
    };

    const triggerFlash = (type) => {
        setFlash(type);
        setTimeout(() => setFlash(null), 1800);
    };

    const cls = [
        'watch-btn',
        `watch-btn--${size}`,
        `watch-btn--${variant}`,
        watched ? 'watch-btn--watching' : '',
        busy ? 'watch-btn--busy' : '',
        flash ? `watch-btn--flash-${flash}` : '',
    ].filter(Boolean).join(' ');

    const label = busy
        ? '...'
        : watched
            ? '✓ Watching'
            : '+ Watch';

    const title = watched
        ? `Remove ${name || 'this asset'} from watchlist`
        : `Add ${name || 'this asset'} to watchlist`;

    return (
        <button
            className={cls}
            onClick={handleClick}
            title={title}
            aria-label={title}
            aria-pressed={watched}
            disabled={busy}
        >
            {label}
        </button>
    );
};

export default WatchButton;
