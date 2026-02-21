import { Loader2 } from 'lucide-react';
import './Loading.css';

export default function Loading({ message = 'Loading...' }) {
    return (
        <div className="loading">
            <Loader2 className="loading-spinner" size={32} />
            <p className="loading-message">{message}</p>
        </div>
    );
}
