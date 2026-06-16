import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { classifyUnusablePdsHost } from './pds-host-policy.js';

void describe('pds host policy', () => {
  void it('flags loopback, private, link-local and reserved addresses', () => {
    assert.equal(classifyUnusablePdsHost('127.0.0.1'), 'loopback');
    assert.equal(classifyUnusablePdsHost('http://127.0.0.1:25000'), 'loopback');
    assert.equal(classifyUnusablePdsHost('localhost:3000'), 'loopback');
    assert.equal(classifyUnusablePdsHost('10.0.0.8'), 'private');
    assert.equal(classifyUnusablePdsHost('172.20.1.2'), 'private');
    assert.equal(classifyUnusablePdsHost('192.168.1.9'), 'private');
    assert.equal(classifyUnusablePdsHost('169.254.2.3'), 'link-local');
    assert.equal(classifyUnusablePdsHost('100.100.10.5'), 'reserved');
    assert.equal(classifyUnusablePdsHost('198.51.100.8'), 'reserved');
    assert.equal(classifyUnusablePdsHost('pds.invalid'), 'reserved');
    assert.equal(classifyUnusablePdsHost('bridge.test'), 'reserved');
    assert.equal(classifyUnusablePdsHost('dev.localhost'), 'loopback');
    assert.equal(classifyUnusablePdsHost('http://[::1]:2583'), 'loopback');
    assert.equal(classifyUnusablePdsHost('::ffff:127.0.0.1'), 'loopback');
    assert.equal(
      classifyUnusablePdsHost('http://[::ffff:127.0.0.1]:8080'),
      'loopback',
    );
    assert.equal(classifyUnusablePdsHost('::ffff:7f00:1'), 'loopback');
    assert.equal(classifyUnusablePdsHost('fd00::1'), 'private');
    assert.equal(classifyUnusablePdsHost('fe80::abcd'), 'link-local');
    assert.equal(classifyUnusablePdsHost('2001:db8::1'), 'reserved');
  });

  void it('leaves public hosts alone', () => {
    assert.equal(classifyUnusablePdsHost('bsky.social'), null);
    assert.equal(classifyUnusablePdsHost('atp.referendumapp.com'), null);
    assert.equal(classifyUnusablePdsHost('https://pds.futur.blue'), null);
  });

  void it('treats malformed endpoints as unusable', () => {
    assert.equal(classifyUnusablePdsHost('http://['), 'invalid');
  });
});
