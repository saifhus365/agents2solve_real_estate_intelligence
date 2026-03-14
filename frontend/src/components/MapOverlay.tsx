import { useEffect, useRef } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

interface MapOverlayProps {
    highlightedAreas: string[];
}

// Hardcoded area polygon centroids for MVP
// In production, use a full GeoJSON file with actual boundaries
const AREA_COORDINATES: Record<string, [number, number]> = {
    'Dubai Marina': [55.1392, 25.0771],
    'Downtown Dubai': [55.2708, 25.1972],
    'Business Bay': [55.2625, 25.1866],
    'JVC': [55.2100, 25.0600],
    'Jumeirah Village Circle': [55.2100, 25.0600],
    'Palm Jumeirah': [55.1344, 25.1124],
    'Dubai Hills': [55.2400, 25.1000],
    'Dubai Hills Estate': [55.2400, 25.1000],
    'JBR': [55.1340, 25.0800],
    'Jumeirah Beach Residence': [55.1340, 25.0800],
    'Arabian Ranches': [55.2700, 25.0600],
    'DAMAC Hills': [55.2500, 25.0300],
    'Dubai Creek Harbour': [55.3400, 25.1950],
    'MBR City': [55.3100, 25.1600],
    'Al Barsha': [55.2100, 25.1100],
    'Sports City': [55.2200, 25.0400],
    'Motor City': [55.2300, 25.0480],
    'International City': [55.3900, 25.1600],
    'Discovery Gardens': [55.1200, 25.0500],
    'JLT': [55.1500, 25.0750],
    'Jumeirah Lake Towers': [55.1500, 25.0750],
    'Dubai South': [55.1500, 24.9600],
    'Town Square': [55.2700, 25.0100],
    'Al Quoz': [55.2200, 25.1500],
    'Bur Dubai': [55.3022, 25.2529],
    'Media City': [55.1540, 25.0920],
    'Knowledge Village': [55.1610, 25.0990],
};

export default function MapOverlay({ highlightedAreas }: MapOverlayProps) {
    const mapContainer = useRef<HTMLDivElement>(null);
    const mapRef = useRef<maplibregl.Map | null>(null);
    const markersRef = useRef<maplibregl.Marker[]>([]);

    useEffect(() => {
        if (!mapContainer.current) return;

        const map = new maplibregl.Map({
            container: mapContainer.current,
            style: {
                version: 8,
                sources: {
                    'osm-tiles': {
                        type: 'raster',
                        tiles: [
                            'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
                        ],
                        tileSize: 256,
                        attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
                    },
                },
                layers: [
                    {
                        id: 'osm-tiles',
                        type: 'raster',
                        source: 'osm-tiles',
                        minzoom: 0,
                        maxzoom: 19,
                    },
                ],
            },
            center: [55.2708, 25.2048],
            zoom: 11,
            attributionControl: false,
        });

        map.addControl(new maplibregl.NavigationControl(), 'top-right');
        map.addControl(
            new maplibregl.AttributionControl({ compact: true }),
            'bottom-right'
        );

        mapRef.current = map;

        return () => {
            map.remove();
        };
    }, []);

    // Update markers when highlighted areas change
    useEffect(() => {
        if (!mapRef.current) return;

        // Remove old markers
        markersRef.current.forEach(m => m.remove());
        markersRef.current = [];

        if (highlightedAreas.length === 0) return;

        const bounds = new maplibregl.LngLatBounds();

        highlightedAreas.forEach(area => {
            const coords = AREA_COORDINATES[area];
            if (!coords) return;

            // Create a pulsing marker element
            const el = document.createElement('div');
            el.style.cssText = `
        width: 24px;
        height: 24px;
        background: radial-gradient(circle, #C9A84C 0%, rgba(201,168,76,0.3) 70%, transparent 100%);
        border-radius: 50%;
        border: 2px solid #C9A84C;
        box-shadow: 0 0 15px rgba(201,168,76,0.5);
        animation: pulse 2s infinite;
        cursor: pointer;
      `;

            const marker = new maplibregl.Marker({ element: el })
                .setLngLat(coords)
                .setPopup(
                    new maplibregl.Popup({ offset: 15, closeButton: false })
                        .setHTML(`<strong>${area}</strong>`)
                )
                .addTo(mapRef.current!);

            markersRef.current.push(marker);
            bounds.extend(coords);
        });

        // Fit map to show all highlighted areas
        if (highlightedAreas.length === 1) {
            const coords = AREA_COORDINATES[highlightedAreas[0]];
            if (coords) {
                mapRef.current.flyTo({ center: coords, zoom: 13, duration: 1500 });
            }
        } else {
            mapRef.current.fitBounds(bounds, { padding: 80, duration: 1500 });
        }
    }, [highlightedAreas]);

    return (
        <div className="relative w-full h-full">
            <div ref={mapContainer} className="w-full h-full" />

            {/* Overlay: area labels */}
            {highlightedAreas.length > 0 && (
                <div className="absolute top-4 left-4 glass rounded-xl px-4 py-3 max-w-[220px]">
                    <div className="text-xs font-semibold text-dubai-gold mb-2">
                        📍 Highlighted Areas
                    </div>
                    <div className="flex flex-wrap gap-1.5">
                        {highlightedAreas.map(area => (
                            <span
                                key={area}
                                className="text-[10px] bg-dubai-gold/20 text-dubai-gold px-2 py-0.5 rounded-full border border-dubai-gold/30"
                            >
                                {area}
                            </span>
                        ))}
                    </div>
                </div>
            )}

            {/* Map legend */}
            <div className="absolute bottom-4 left-4 glass rounded-lg px-3 py-2">
                <div className="text-[10px] text-gray-400">
                    Dubai Real Estate Map • Centred: 25.20°N, 55.27°E
                </div>
            </div>

            {/* Add pulse keyframes via style tag */}
            <style>{`
        @keyframes pulse {
          0% { transform: scale(1); opacity: 1; }
          50% { transform: scale(1.5); opacity: 0.7; }
          100% { transform: scale(1); opacity: 1; }
        }
      `}</style>
        </div>
    );
}
