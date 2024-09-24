/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        beige: {
          100: '#f7fafc',
          200: '#e0e3e5',
        },
        gray: {
          100: '#f7fafc',
          700: '#a0aec0',
          800: '#2d3748',
          900: '#1a202c',
        },
        blue: {
          500: '#3b82f6',
          600: '#2563eb',
        },
      },
    },
  },
  plugins: [],
};
