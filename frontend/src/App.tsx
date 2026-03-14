import { useState } from 'react';
import ChatPanel from './components/ChatPanel';
import MapOverlay from './components/MapOverlay';

export default function App() {
    const [highlightedAreas, setHighlightedAreas] = useState<string[]>([]);
    const [isMapExpanded, setIsMapExpanded] = useState(false);

    return (
        <div className="h-screen w-screen flex flex-col overflow-hidden bg-dubai-night">
            {/* Top bar */}
            <header className="h-12 flex items-center justify-between px-6 border-b border-white/5 bg-black/20 shrink-0">
                <div className="flex items-center gap-3">
                    <div className="w-2 h-2 rounded-full bg-dubai-gold animate-pulse-slow" />
                    <span className="text-xs font-medium text-gray-400 tracking-wider uppercase">
                        Dubai Real Estate Intelligence Co-Pilot
                    </span>
                </div>
                <div className="flex items-center gap-4">
                    <button
                        onClick={() => setIsMapExpanded(!isMapExpanded)}
                        className="text-xs text-gray-500 hover:text-white transition-colors"
                    >
                        {isMapExpanded ? '◀ Show Chat' : 'Expand Map ▶'}
                    </button>
                    <div className="flex items-center gap-2">
                        <span className="text-[10px] text-gray-600">v1.0.0</span>
                        <div className="w-1.5 h-1.5 rounded-full bg-emerald-500" title="System healthy" />
                    </div>
                </div>
            </header>

            {/* Main content: split pane */}
            <div className="flex-1 flex overflow-hidden">
                {/* Chat Panel */}
                <div
                    className={`
            transition-all duration-500 ease-in-out overflow-hidden
            ${isMapExpanded ? 'w-0' : 'w-full lg:w-[55%]'}
            border-r border-white/5
          `}
                >
                    <ChatPanel onAreasMentioned={setHighlightedAreas} />
                </div>

                {/* Map Panel */}
                <div
                    className={`
            transition-all duration-500 ease-in-out
            ${isMapExpanded ? 'w-full' : 'hidden lg:block lg:w-[45%]'}
          `}
                >
                    <MapOverlay highlightedAreas={highlightedAreas} />
                </div>
            </div>
        </div>
    );
}
