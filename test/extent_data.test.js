import { describe, it, expect } from 'vitest';
import { ExtentData, extractLabel } from '../src/extent_data.js';

describe('extractLabel', () => {
  it('extracts numeric suffix from partition filename', () => {
    expect(extractLabel('data.0.parquet')).toBe('0');
    expect(extractLabel('data.42.parquet')).toBe('42');
  });

  it('extracts numeric suffix without .parquet', () => {
    expect(extractLabel('data.7')).toBe('7');
  });

  it('extracts row group key', () => {
    expect(extractLabel('rg_0')).toBe('0');
    expect(extractLabel('rg_15')).toBe('15');
  });

  it('returns null for non-numeric names', () => {
    expect(extractLabel('data')).toBeNull();
    expect(extractLabel('file.parquet')).toBeNull();
  });
});

describe('ExtentData', () => {
  it('requires metadataProvider', () => {
    expect(() => new ExtentData({})).toThrow('metadataProvider is required');
  });

  it('toGeoJSON converts extents to FeatureCollections', () => {
    const ed = new ExtentData({
      metadataProvider: { getExtents: async () => ({}) },
    });

    const extents = {
      'data.0.parquet': [77.0, 12.0, 78.0, 13.0],
      'data.1.parquet': [78.0, 12.0, 79.0, 13.0],
    };

    const { polygons, labelPoints } = ed.toGeoJSON(extents);

    expect(polygons.type).toBe('FeatureCollection');
    expect(polygons.features).toHaveLength(2);
    expect(polygons.features[0].geometry.type).toBe('Polygon');
    expect(polygons.features[0].properties.name).toBe('data.0.parquet');
    expect(polygons.features[0].properties.label).toBe('0');

    expect(labelPoints.type).toBe('FeatureCollection');
    expect(labelPoints.features).toHaveLength(2);
    expect(labelPoints.features[0].geometry.type).toBe('Point');
  });

  it('toGeoJSON handles null extents', () => {
    const ed = new ExtentData({
      metadataProvider: { getExtents: async () => ({}) },
    });

    const { polygons, labelPoints } = ed.toGeoJSON(null);
    expect(polygons.features).toHaveLength(0);
    expect(labelPoints.features).toHaveLength(0);
  });
});
