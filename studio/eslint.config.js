// Lint rules for Studio: typescript-eslint's type-checked rules, React hooks,
// and the security rules chapter 44 asks for (no eval, no HTML injection).
import js from '@eslint/js';
import globals from 'globals';
import reactHooks from 'eslint-plugin-react-hooks';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  { ignores: ['dist', 'node_modules', 'test-results', 'playwright-report', 'src/api/gen'] },
  js.configs.recommended,
  ...tseslint.configs.recommendedTypeChecked,
  {
    languageOptions: {
      globals: { ...globals.browser, ...globals.node },
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
    },
    plugins: { 'react-hooks': reactHooks },
    rules: {
      ...reactHooks.configs.recommended.rules,
      'no-eval': 'error',
      'no-implied-eval': 'error',
      'no-new-func': 'error',
      'no-restricted-properties': [
        'error',
        { property: 'innerHTML', message: 'Render data as text (chapter 44, "Security notes").' },
        { property: 'outerHTML', message: 'Render data as text.' },
      ],
      'no-restricted-syntax': [
        'error',
        {
          selector: "JSXAttribute[name.name='dangerouslySetInnerHTML']",
          message: 'Render data as text (chapter 44, "Security notes").',
        },
      ],
      'no-restricted-globals': [
        'error',
        { name: 'localStorage', message: 'Use src/lib/prefs.ts (never for tokens or results).' },
        { name: 'sessionStorage', message: 'Studio keeps no session data in storage.' },
      ],
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      '@typescript-eslint/no-misused-promises': ['error', { checksVoidReturn: { attributes: false } }],
      // TanStack Virtual: the compiler skips memoizing that component, which is fine.
      'react-hooks/incompatible-library': 'off',
    },
  },
  { files: ['**/*.mjs', 'eslint.config.js'], ...tseslint.configs.disableTypeChecked },
  {
    files: ['scripts/**', 'e2e/**', '*.config.*', 'eslint.config.js'],
    rules: { 'no-restricted-globals': 'off' },
  },
);
