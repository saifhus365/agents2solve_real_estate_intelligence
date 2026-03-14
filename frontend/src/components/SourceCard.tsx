import { Citation } from '../hooks/useStreamChat';

interface SourceCardProps {
    citation: Citation;
    index: number;
}

export default function SourceCard({ citation, index }: SourceCardProps) {
    const typeColors: Record<string, string> = {
        graph: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
        vector: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
        sql: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    };

    const typeIcons: Record<string, string> = {
        graph: '🔗',
        vector: '📄',
        sql: '📊',
    };

    const colorClass = typeColors[citation.source_type] || typeColors.vector;
    const icon = typeIcons[citation.source_type] || '📄';

    return (
        <a
            href={citation.url || '#'}
            target="_blank"
            rel="noopener noreferrer"
            className={`
        inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs
        border transition-all duration-200
        hover:scale-105 hover:shadow-lg cursor-pointer
        ${colorClass}
      `}
        >
            <span className="text-sm">{icon}</span>
            <span className="font-medium truncate max-w-[180px]">
                [{index + 1}] {citation.source_name}
            </span>
            <span className="opacity-60 text-[10px] uppercase tracking-wider">
                {citation.source_type}
            </span>
        </a>
    );
}
