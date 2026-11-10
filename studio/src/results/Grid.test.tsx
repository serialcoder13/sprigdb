import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Grid } from './Grid';

describe('Grid', () => {
  it('renders the ARIA grid with text-only values', () => {
    const data = [
      ['1', '<img src=x onerror=alert(1)>'],
      ['2', null],
    ];
    render(<Grid label="r" columns={['id', 'note']} rowCount={2} cell={(r, c) => data[r]![c] ?? null} />);
    const grid = screen.getByRole('grid', { name: 'r' });
    expect(grid).toHaveAttribute('aria-rowcount', '3');
    expect(screen.getAllByRole('columnheader').map((h) => h.textContent)).toEqual(['#', 'id', 'note']);
    // XSS through cell values: shown as text, no element created.
    expect(document.querySelector('img')).toBeNull();
  });
});
