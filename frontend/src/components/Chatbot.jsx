import { useState, useRef, useEffect } from 'react';
import './Chatbot.css';

const API_BASE = 'http://localhost:8000/api/v1';

// Suggested starter questions shown in the chat panel
const SUGGESTIONS = [
    "What is the market cap of Reliance?",
    "Top 5 holdings of Quant Small Cap Fund?",
    "Which funds hold Infosys?",
    "How many shares of HDFC Bank does SBI Blue Chip Fund hold?",
];

function MarkdownText({ text }) {
    // Very simple markdown: **bold**, newlines
    const parts = text.split(/(\*\*[^*]+\*\*|\n)/g);
    return (
        <span>
            {parts.map((part, i) => {
                if (part === '\n') return <br key={i} />;
                if (part.startsWith('**') && part.endsWith('**')) {
                    return <strong key={i}>{part.slice(2, -2)}</strong>;
                }
                return part;
            })}
        </span>
    );
}

export default function Chatbot() {
    const [isOpen, setIsOpen] = useState(false);
    const [messages, setMessages] = useState([
        {
            role: 'bot',
            text: "👋 Hi! I'm your MF Analytics assistant. Ask me anything about mutual fund holdings, market caps, or top stocks in a scheme!",
        },
    ]);
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [showSuggestions, setShowSuggestions] = useState(true);
    const messagesEndRef = useRef(null);
    const inputRef = useRef(null);

    useEffect(() => {
        if (isOpen && messagesEndRef.current) {
            messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
        }
    }, [messages, isLoading, isOpen]);

    useEffect(() => {
        if (isOpen && inputRef.current) {
            setTimeout(() => inputRef.current?.focus(), 300);
        }
    }, [isOpen]);

    const sendMessage = async (text) => {
        const trimmed = (text || input).trim();
        if (!trimmed || isLoading) return;

        setShowSuggestions(false);
        setInput('');
        setMessages((prev) => [...prev, { role: 'user', text: trimmed }]);
        setIsLoading(true);

        try {
            const res = await fetch(`${API_BASE}/chat`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: trimmed }),
            });

            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                if (res.status === 429) {
                    throw new Error('⏳ Rate limit reached. Please wait ~30 seconds and try again.');
                }
                throw new Error(err.detail || `Server error ${res.status}`);
            }

            const data = await res.json();
            setMessages((prev) => [...prev, { role: 'bot', text: data.reply }]);
        } catch (err) {
            setMessages((prev) => [
                ...prev,
                {
                    role: 'bot',
                    text: `⚠️ Sorry, I ran into an error: ${err.message}. Please try again.`,
                    isError: true,
                },
            ]);
        } finally {
            setIsLoading(false);
        }
    };

    const handleKeyDown = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    };

    const handleSuggestion = (text) => {
        sendMessage(text);
    };

    const handleToggle = () => {
        setIsOpen((prev) => !prev);
    };

    return (
        <div className="chatbot-root">
            {/* Floating bubble */}
            <button
                className={`chatbot-bubble ${isOpen ? 'chatbot-bubble--open' : ''}`}
                onClick={handleToggle}
                aria-label="Open AI Assistant"
            >
                <span className="chatbot-bubble__icon">
                    {isOpen ? (
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                            <line x1="18" y1="6" x2="6" y2="18" />
                            <line x1="6" y1="6" x2="18" y2="18" />
                        </svg>
                    ) : (
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                            <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
                            <circle cx="9" cy="10" r="1" fill="currentColor" />
                            <circle cx="12" cy="10" r="1" fill="currentColor" />
                            <circle cx="15" cy="10" r="1" fill="currentColor" />
                        </svg>
                    )}
                </span>
                {!isOpen && <span className="chatbot-bubble__ping" />}
            </button>

            {/* Chat panel */}
            <div className={`chatbot-panel ${isOpen ? 'chatbot-panel--open' : ''}`}>
                {/* Header */}
                <div className="chatbot-header">
                    <div className="chatbot-header__info">
                        <div className="chatbot-header__avatar">AI</div>
                        <div>
                            <div className="chatbot-header__name">MF Assistant</div>
                            <div className="chatbot-header__status">
                                <span className="chatbot-header__dot" /> Powered by Groq
                            </div>
                        </div>
                    </div>
                    <button className="chatbot-header__close" onClick={handleToggle} aria-label="Close chat">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="18" height="18">
                            <line x1="18" y1="6" x2="6" y2="18" />
                            <line x1="6" y1="6" x2="18" y2="18" />
                        </svg>
                    </button>
                </div>

                {/* Messages */}
                <div className="chatbot-messages">
                    {messages.map((msg, i) => (
                        <div
                            key={i}
                            className={`chatbot-msg chatbot-msg--${msg.role} ${msg.isError ? 'chatbot-msg--error' : ''}`}
                        >
                            {msg.role === 'bot' && (
                                <div className="chatbot-msg__avatar">AI</div>
                            )}
                            <div className="chatbot-msg__bubble">
                                <MarkdownText text={msg.text} />
                            </div>
                        </div>
                    ))}

                    {/* Typing indicator */}
                    {isLoading && (
                        <div className="chatbot-msg chatbot-msg--bot">
                            <div className="chatbot-msg__avatar">AI</div>
                            <div className="chatbot-msg__bubble chatbot-msg__bubble--typing">
                                <span className="dot" />
                                <span className="dot" />
                                <span className="dot" />
                            </div>
                        </div>
                    )}
                    <div ref={messagesEndRef} />
                </div>

                {/* Suggestions */}
                {showSuggestions && (
                    <div className="chatbot-suggestions">
                        <p className="chatbot-suggestions__label">Try asking:</p>
                        <div className="chatbot-suggestions__list">
                            {SUGGESTIONS.map((s, i) => (
                                <button key={i} className="chatbot-suggestion-chip" onClick={() => handleSuggestion(s)}>
                                    {s}
                                </button>
                            ))}
                        </div>
                    </div>
                )}

                {/* Input area */}
                <div className="chatbot-input-area">
                    <textarea
                        ref={inputRef}
                        className="chatbot-input"
                        placeholder="Ask about holdings, market cap, top stocks..."
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        onKeyDown={handleKeyDown}
                        rows={1}
                        disabled={isLoading}
                    />
                    <button
                        className={`chatbot-send ${input.trim() && !isLoading ? 'chatbot-send--active' : ''}`}
                        onClick={() => sendMessage()}
                        disabled={!input.trim() || isLoading}
                        aria-label="Send message"
                    >
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" width="18" height="18">
                            <line x1="22" y1="2" x2="11" y2="13" />
                            <polygon points="22 2 15 22 11 13 2 9 22 2" />
                        </svg>
                    </button>
                </div>
            </div>
        </div>
    );
}
