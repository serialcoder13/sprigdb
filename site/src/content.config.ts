import { defineCollection } from 'astro:content';
import { z } from 'astro/zod';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';

// docs/ pages carry an audience, and generated pages say where they come from.
export const collections = {
  docs: defineCollection({
    loader: docsLoader(),
    schema: docsSchema({
      extend: z.object({
        audience: z.array(z.enum(['user', 'dba', 'operator'])).optional(),
        generated: z.boolean().optional(),
        source: z.string().optional(),
        since: z.string().optional(),
      }),
    }),
  }),
};
