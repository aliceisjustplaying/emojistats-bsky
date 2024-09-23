/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        beige: {
          100: '#f7fafc', // Beige color
          200: '#e0e3e5', // Beige color (10% darker)
        },
        gray: {
          100: '#f7fafc', // Light Gray for emoji backgrounds
          700: '#a0aec0', // Adjusted Gray if needed
          800: '#2d3748', // Adjusted Gray for text
        },
      },
    },
  },
  plugins: [],
};
