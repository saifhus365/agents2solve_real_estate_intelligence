interface QueryExamplesProps {
    onSelect: (query: string) => void;
}

const EXAMPLES = [
    {
        query:
            'What is the average price per sqft for 2-bed apartments in JVC over the last 6 months compared to last year?',
        icon: '📈',
        label: 'JVC Price Trends',
    },
    {
        query:
            'Which developers delivered projects on time in 2023 and 2024, and where are their next off-plan launches?',
        icon: '🏗️',
        label: 'Developer Track Records',
    },
    {
        query:
            'Show me all areas within 800m of a metro station where off-plan transaction ratio is increasing.',
        icon: '🚇',
        label: 'Metro Proximity Analysis',
    },
    {
        query:
            'What did analysts say about Dubai Marina supply risk last month?',
        icon: '📰',
        label: 'Marina Market Sentiment',
    },
    {
        query:
            'Which 3-bed villas in Dubai Hills have transacted more than twice? What was the flip timeline and gain?',
        icon: '🏡',
        label: 'Dubai Hills Flip Analysis',
    },
    {
        query:
            'Is Emaar still the dominant developer in Downtown or has that changed?',
        icon: '🏙️',
        label: 'Downtown Developer Landscape',
    },
];

export default function QueryExamples({ onSelect }: QueryExamplesProps) {
    return (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 max-w-2xl mx-auto">
            {EXAMPLES.map((ex, i) => (
                <button
                    key={i}
                    onClick={() => onSelect(ex.query)}
                    className="
            glass rounded-xl p-4 text-left
            transition-all duration-200
            hover:bg-white/10 hover:border-dubai-gold/40 hover:scale-[1.02]
            active:scale-[0.98]
            group cursor-pointer
          "
                >
                    <div className="flex items-start gap-3">
                        <span className="text-2xl mt-0.5 group-hover:scale-110 transition-transform">
                            {ex.icon}
                        </span>
                        <div>
                            <div className="font-semibold text-dubai-gold text-sm mb-1">
                                {ex.label}
                            </div>
                            <div className="text-xs text-gray-400 leading-relaxed line-clamp-2">
                                {ex.query}
                            </div>
                        </div>
                    </div>
                </button>
            ))}
        </div>
    );
}
