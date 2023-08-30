import { defineConfig } from 'tsup';
export default defineConfig({
    entry: {
        index: "src/adapter.js"
    },
    outDir: 'dist',
    target: 'node22',
    format: ['esm'],
    treeshake: true,
    clean: true,
    minify: false,
    sourcemap: "inline"
});