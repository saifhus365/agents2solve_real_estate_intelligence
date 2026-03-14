import { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import { useStreamChat, ChatMessage } from '../hooks/useStreamChat';
import SourceCard from './SourceCard';
import QueryExamples from './QueryExamples';

interface ChatPanelProps {
    onAreasMentioned?: (areas: string[]) => void;
}

const KNOWN_AREAS = [
    'Dubai Marina', 'Downtown Dubai', 'Business Bay', 'JVC',
    'Jumeirah Village Circle', 'Palm Jumeirah', 'Dubai Hills',
    'Dubai Hills Estate', 'JBR', 'Jumeirah Beach Residence',
    'Arabian Ranches', 'DAMAC Hills', 'Dubai Creek Harbour',
    'MBR City', 'Al Barsha', 'Sports City', 'Motor City',
    'International City', 'Discovery Gardens', 'JLT',
    'Jumeirah Lake Towers', 'Dubai South', 'Town Square',
    'Al Quoz', 'Bur Dubai', 'Media City', 'Knowledge Village',
];

function extractAreas(text: string): string[] {
    const lower = text.toLowerCase();
    return KNOWN_AREAS.filter(a => lower.includes(a.toLowerCase()));
}

export default function ChatPanel({ onAreasMentioned }: ChatPanelProps) {
    const { messages, isLoading, sendMessage, clearMessages } = useStreamChat();
    const [input, setInput] = useState('');
    const bottomRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [messages]);

    // Notify parent about mentioned areas
    useEffect(() => {
        if (!onAreasMentioned) return;
        const lastAssistant = [...messages].reverse().find(m => m.role === 'assistant' && !m.isStreaming);
        if (lastAssistant) {
            const areas = extractAreas(lastAssistant.content);
            if (areas.length > 0) onAreasMentioned(areas);
        }
    }, [messages, onAreasMentioned]);

    const handleSubmit = (e: React.FormEvent) => {
        e.preventDefault();
        if (input.trim()) {
            sendMessage(input);
            setInput('');
        }
    };

    const handleExampleSelect = (query: string) => {
        sendMessage(query);
    };

    const hasMessages = messages.length > 0;

    return (
        <div className="flex flex-col h-full">
            {/* Header */}
            <div className="px-5 py-4 border-b border-white/10 flex items-center justify-between">
                <div>
                    <h1 className="text-lg font-bold bg-gradient-to-r from-dubai-gold to-amber-300 bg-clip-text text-transparent">
                        Dubai RE Co-Pilot
                    </h1>
                    <p className="text-xs text-gray-500 mt-0.5">GraphRAG Intelligence</p>
                </div>
                {hasMessages && (
                    <button
                        onClick={clearMessages}
                        className="text-xs text-gray-500 hover:text-white transition-colors px-2 py-1 rounded hover:bg-white/5"
                    >
                        Clear Chat
                    </button>
                )}
            </div>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
                {!hasMessages && (
                    <div className="flex flex-col items-center justify-center h-full gap-6">
                        <div className="text-center">
                            <div className="text-4xl mb-3">🏙️</div>
                            <h2 className="text-xl font-semibold text-white mb-1">
                                Dubai Real Estate Intelligence
                            </h2>
                            <p className="text-sm text-gray-400 max-w-md">
                                Ask me anything about Dubai property market — prices, developers,
                                areas, transactions, or market sentiment.
                            </p>
                        </div>
                        <QueryExamples onSelect={handleExampleSelect} />
                    </div>
                )}

                {messages.map((msg: ChatMessage) => (
                    <div
                        key={msg.id}
                        className={`animate-slide-up ${msg.role === 'user' ? 'flex justify-end' : ''
                            }`}
                    >
                        {msg.role === 'user' ? (
                            <div className="max-w-[80%] bg-primary-600/30 border border-primary-500/20 rounded-2xl rounded-br-md px-4 py-3">
                                <p className="text-sm text-gray-100">{msg.content}</p>
                            </div>
                        ) : (
                            <div className="max-w-[95%]">
                                <div className="glass rounded-2xl rounded-bl-md px-4 py-3">
                                    {msg.content ? (
                                        <div className="chat-markdown text-sm">
                                            <ReactMarkdown>{msg.content}</ReactMarkdown>
                                        </div>
                                    ) : msg.isStreaming ? (
                                        <div className="flex items-center gap-1.5 py-2">
                                            <span className="typing-dot" />
                                            <span className="typing-dot" />
                                            <span className="typing-dot" />
                                        </div>
                                    ) : null}

                                    {/* Latency badge */}
                                    {msg.latencyMs !== null && !msg.isStreaming && (
                                        <div className="mt-2 text-[10px] text-gray-500">
                                            ⚡ {(msg.latencyMs / 1000).toFixed(1)}s
                                        </div>
                                    )}
                                </div>

                                {/* Citations */}
                                {msg.citations.length > 0 && !msg.isStreaming && (
                                    <div className="flex flex-wrap gap-2 mt-2 ml-1">
                                        {msg.citations.map((c, i) => (
                                            <SourceCard key={i} citation={c} index={i} />
                                        ))}
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                ))}
                <div ref={bottomRef} />
            </div>

            {/* Input */}
            <div className="px-5 py-4 border-t border-white/10">
                <form onSubmit={handleSubmit} className="flex gap-3">
                    <input
                        type="text"
                        value={input}
                        onChange={e => setInput(e.target.value)}
                        placeholder="Ask about Dubai real estate..."
                        className="
              flex-1 glass rounded-xl px-4 py-3 text-sm
              text-white placeholder-gray-500
              focus:outline-none focus:ring-2 focus:ring-dubai-gold/40
              transition-all
            "
                        disabled={isLoading}
                    />
                    <button
                        type="submit"
                        disabled={isLoading || !input.trim()}
                        className="
              px-5 py-3 rounded-xl font-semibold text-sm
              bg-gradient-to-r from-dubai-gold to-amber-500
              text-dubai-night
              hover:shadow-lg hover:shadow-dubai-gold/20
              disabled:opacity-40 disabled:cursor-not-allowed
              transition-all duration-200 active:scale-95
            "
                    >
                        {isLoading ? '...' : 'Send'}
                    </button>
                </form>
            </div>
        </div>
    );
}
