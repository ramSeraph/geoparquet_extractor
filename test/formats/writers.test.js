import { describe, it, expect } from 'vitest';
import {
  promoteGeometry, resolveShpTypeMapping, truncateFieldNames,
  ShpWriter, DbfWriter, PRJ_WGS84, SHP_TYPE_LABELS,
} from '../../src/formats/shp_writer.js';
import { geometryToKml, featureToPlacemark, KML_HEADER, KML_FOOTER } from '../../src/formats/kml_writer.js';
import { createUtmTransform, featureToDxfEntities, buildDxfEnvelope } from '../../src/formats/dxf_writer.js';

describe('shp_writer', () => {
  describe('promoteGeometry', () => {
    it('promotes Point to MultiPoint', () => {
      const geom = promoteGeometry({ type: 'Point', coordinates: [1, 2] });
      expect(geom.type).toBe('MultiPoint');
      expect(geom.coordinates).toEqual([[1, 2]]);
    });

    it('leaves MultiPolygon unchanged', () => {
      const geom = { type: 'MultiPolygon', coordinates: [] };
      expect(promoteGeometry(geom)).toBe(geom);
    });
  });

  describe('resolveShpTypeMapping', () => {
    it('maps single geometry type', () => {
      const { shpTypes, typeMapping } = resolveShpTypeMapping(new Set(['POLYGON']));
      expect(shpTypes).toEqual(['polygon']);
      expect(typeMapping.get('POLYGON').shpType).toBe('polygon');
    });

    it('promotes POINT when MULTIPOINT also present', () => {
      const { typeMapping } = resolveShpTypeMapping(new Set(['POINT', 'MULTIPOINT']));
      expect(typeMapping.get('POINT').needsPromote).toBe(true);
    });
  });

  describe('truncateFieldNames', () => {
    it('truncates names to 10 characters', () => {
      const result = truncateFieldNames(['very_long_field_name']);
      expect(result[0].dbfName.length).toBeLessThanOrEqual(10);
    });

    it('handles duplicate truncated names', () => {
      const result = truncateFieldNames(['abcdefghij1', 'abcdefghij2']);
      const dbfNames = result.map(r => r.dbfName);
      expect(new Set(dbfNames).size).toBe(2);
    });
  });

  describe('ShpWriter', () => {
    it('writes a point record', () => {
      const writer = new ShpWriter('point');
      writer.writeRecord({ type: 'Point', coordinates: [1, 2] });
      expect(writer.recNum).toBe(1);
      const chunks = writer.flushChunks();
      expect(chunks).toHaveLength(1);
      expect(chunks[0]).toBeInstanceOf(Uint8Array);
    });
  });

  describe('DbfWriter', () => {
    it('writes a record', () => {
      const writer = new DbfWriter([
        { dbfName: 'name', originalName: 'name', type: 'character' },
        { dbfName: 'value', originalName: 'value', type: 'number' },
      ]);
      writer.writeRecord({ name: 'test', value: 42 });
      expect(writer.records).toBe(1);
    });
  });
});

describe('kml_writer', () => {
  it('converts Point to KML', () => {
    const kml = geometryToKml({ type: 'Point', coordinates: [1, 2] });
    expect(kml).toContain('<Point>');
    expect(kml).toContain('1,2');
  });

  it('featureToPlacemark generates valid XML', () => {
    const geom = { type: 'Point', coordinates: [77, 13] };
    const result = featureToPlacemark(geom, { name: 'Test' }, []);
    expect(result).toContain('<Placemark>');
    expect(result).toContain('<name>Test</name>');
  });

  it('has header and footer', () => {
    expect(KML_HEADER).toContain('<?xml');
    expect(KML_FOOTER).toContain('</kml>');
  });
});

describe('dxf_writer', () => {
  it('createUtmTransform produces valid transform', () => {
    const transform = createUtmTransform(43, 'N');
    const [e, n, alt] = transform([77.5, 13.0, 0]);
    expect(e).toBeGreaterThan(0);
    expect(n).toBeGreaterThan(0);
    expect(alt).toBe(0);
  });

  it('featureToDxfEntities returns DXF string', () => {
    const transform = createUtmTransform(43, 'N');
    const { dxf, layerName } = featureToDxfEntities(
      { type: 'Point', coordinates: [77.5, 13.0] },
      { layer: 'buildings' },
      transform
    );
    expect(dxf).toContain('POINT');
    expect(layerName).toBe('buildings');
  });

  it('buildDxfEnvelope returns header and footer', () => {
    const { header, footer } = buildDxfEnvelope(new Set(['Layer1', 'Layer2']));
    expect(header).toContain('SECTION');
    expect(header).toContain('Layer1');
    expect(footer).toContain('EOF');
  });
});
