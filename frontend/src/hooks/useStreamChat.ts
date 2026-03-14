import { useState, useCallback, useRef } from 'react';

export interface Citation {
    source_name: string;
    url: string | null;
    source_type: string;
    retrieved_at: string;
}

export interface ChatMessage {
    id: string;
    role: 'user' | 'assistant';
    content: string;
    citations: Citation[];
    latencyMs: number | null;
    isStreaming: boolean;
}

interface SSEEvent {
    type: 'token' | 'citations' | 'done' | 'error';
    content?: string;
    data?: Citation[];
    latency_ms?: number;
    message?: string;
}

export function useStreamChat() {
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const abortRef = useRef<AbortController | null>(null);
    const sessionId = useRef(crypto.randomUUID());

    const sendMessage = useCallback(async (query: string) => {
        if (!query.trim() || isLoading) return;

        // Add user message
        const userMsg: ChatMessage = {
            id: crypto.randomUUID(),
            role: 'user',
            content: query,
            citations: [],
            latencyMs: null,
            isStreaming: false,
        };

        const assistantId = crypto.randomUUID();
        const assistantMsg: ChatMessage = {
            id: assistantId,
            role: 'assistant',
            content: '',
            citations: [],
            latencyMs: null,
            isStreaming: true,
        };

        setMessages(prev => [...prev, userMsg, assistantMsg]);
        setIsLoading(true);

        abortRef.current = new AbortController();

        try {
            const resp = await fetch('/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    query,
                    session_id: sessionId.current,
                }),
                signal: abortRef.current.signal,
            });

            if (!resp.ok) {
                throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
            }

            const reader = resp.body?.getReader();
            if (!reader) throw new Error('No response body');

            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';

                for (const line of lines) {
                    if (!line.startsWith('data: ')) continue;
                    const jsonStr = line.slice(6).trim();
                    if (!jsonStr) continue;

                    try {
                        const event: SSEEvent = JSON.parse(jsonStr);

                        if (event.type === 'token' && event.content) {
                            setMessages(prev =>
                                prev.map(m =>
                                    m.id === assistantId
                                        ? { ...m, content: m.content + event.content }
                                        : m
                                )
                            );
                        } else if (event.type === 'citations' && event.data) {
                            setMessages(prev =>
                                prev.map(m =>
                                    m.id === assistantId
                                        ? { ...m, citations: event.data! }
                                        : m
                                )
                            );
                        } else if (event.type === 'done') {
                            setMessages(prev =>
                                prev.map(m =>
                                    m.id === assistantId
                                        ? { ...m, isStreaming: false, latencyMs: event.latency_ms ?? null }
                                        : m
                                )
                            );
                        } else if (event.type === 'error') {
                            setMessages(prev =>
                                prev.map(m =>
                                    m.id === assistantId
                                        ? {
                                            ...m,
                                            content: `⚠️ Error: ${event.message || 'Unknown error'}`,
                                            isStreaming: false,
                                        }
                                        : m
                                )
                            );
                        }
                    } catch {
                        // Skip malformed JSON lines
                    }
                }
            }
        } catch (err) {
            if ((err as Error).name !== 'AbortError') {
                setMessages(prev =>
                    prev.map(m =>
                        m.id === assistantId
                            ? {
                                ...m,
                                content: `⚠️ Connection error: ${(err as Error).message}`,
                                isStreaming: false,
                            }
                            : m
                    )
                );
            }
        } finally {
            setIsLoading(false);
            abortRef.current = null;
        }
    }, [isLoading]);

    const stopStreaming = useCallback(() => {
        abortRef.current?.abort();
        setIsLoading(false);
    }, []);

    const clearMessages = useCallback(() => {
        setMessages([]);
        sessionId.current = crypto.randomUUID();
    }, []);

    return { messages, isLoading, sendMessage, stopStreaming, clearMessages };
}
