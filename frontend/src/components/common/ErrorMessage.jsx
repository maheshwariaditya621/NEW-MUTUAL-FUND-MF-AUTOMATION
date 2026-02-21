import { AlertCircle } from 'lucide-react';
import './ErrorMessage.css';

export default function ErrorMessage({ message, onRetry }) {
    return (
        <div className="error-message">
            <AlertCircle className="error-icon" size={32} />
            <p className="error-text">{message}</p>
            {onRetry && (
                <button className="error-retry-btn" onClick={onRetry}>
                    Try Again
                </button>
            )}
        </div>
    );
}
