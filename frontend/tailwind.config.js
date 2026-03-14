/** @type {import('tailwindcss').Config} */
export default {
    content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
    theme: {
        extend: {
            colors: {
                primary: {
                    50: '#f0f7ff',
                    100: '#e0eefe',
                    200: '#baddfd',
                    300: '#7dc2fc',
                    400: '#38a3f8',
                    500: '#0e87e9',
                    600: '#026ac7',
                    700: '#0355a1',
                    800: '#074985',
                    900: '#0c3d6e',
                },
                dubai: {
                    gold: '#C9A84C',
                    sand: '#E8D5B7',
                    night: '#0F1923',
                    sea: '#1A6B8A',
                    sky: '#87CEEB',
                },
            },
            fontFamily: {
                sans: ['Inter', 'system-ui', 'sans-serif'],
            },
            animation: {
                'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
                'fade-in': 'fadeIn 0.3s ease-in-out',
                'slide-up': 'slideUp 0.3s ease-out',
            },
            keyframes: {
                fadeIn: {
                    '0%': { opacity: '0' },
                    '100%': { opacity: '1' },
                },
                slideUp: {
                    '0%': { opacity: '0', transform: 'translateY(10px)' },
                    '100%': { opacity: '1', transform: 'translateY(0)' },
                },
            },
        },
    },
    plugins: [],
};
