import './Card.css';

export default function Card({ children, className = '', onClick, hoverable = false }) {
    const cardClasses = `card ${hoverable ? 'card-hoverable' : ''} ${className}`;

    return (
        <div className={cardClasses} onClick={onClick}>
            {children}
        </div>
    );
}
