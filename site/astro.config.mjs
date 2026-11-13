// The SprigDB website: a showcase home page and the product documentation
// (decision AI16; chapter 55, "The website and the PDF"). The docs pages
// come from ../docs, copied in by scripts/sync-docs.mjs, so the repository
// has one source for the website, the PDF and the docs index.
import { defineConfig, passthroughImageService } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://serialcoder13.github.io',
  base: '/sprigdb',
  // No sharp: its libvips binary is LGPL-3.0 (brief rule 14). Images are
  // shipped pre-sized.
  image: { service: passthroughImageService() },
  integrations: [
    starlight({
      title: 'SprigDB',
      description:
        'A multi-tenant database gateway over MariaDB: MySQL, MariaDB, MongoDB, Redis and graph clients, automatic sharding and indexing, across data centres.',
      logo: { src: './src/assets/mascot.webp', alt: 'Sprig, the SprigDB axolotl' },
      favicon: '/favicon.png',
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/serialcoder13/sprigdb' }],
      customCss: ['./src/styles/sprig.css'],
      lastUpdated: false,
      sidebar: [
        { label: 'Start here', items: [{ label: 'About these docs', link: '/docs/' }] },
        { label: 'Concepts', collapsed: false, items: [{ autogenerate: { directory: 'docs/concepts' } }] },
        { label: 'Get started', collapsed: false, items: [{ autogenerate: { directory: 'docs/get-started' } }] },
        { label: 'Connect', collapsed: true, items: [{ autogenerate: { directory: 'docs/connect' } }] },
        { label: 'SQL', collapsed: true, items: [{ autogenerate: { directory: 'docs/sql' } }] },
        { label: 'Data models', collapsed: true, items: [{ autogenerate: { directory: 'docs/data-models' } }] },
        { label: 'Performance', collapsed: true, items: [{ autogenerate: { directory: 'docs/performance' } }] },
        { label: 'Security', collapsed: true, items: [{ autogenerate: { directory: 'docs/security' } }] },
        { label: 'Operate', collapsed: true, items: [{ autogenerate: { directory: 'docs/operate' } }] },
        { label: 'Tools', collapsed: true, items: [{ autogenerate: { directory: 'docs/tools' } }] },
        { label: 'Troubleshooting', collapsed: true, items: [{ autogenerate: { directory: 'docs/troubleshooting' } }] },
        { label: 'Reference', collapsed: true, items: [{ autogenerate: { directory: 'docs/reference' } }] },
      ],
    }),
  ],
});
