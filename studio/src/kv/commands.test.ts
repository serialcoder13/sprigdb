import { describe, expect, it } from 'vitest';
import { CommandError, commandClass, formatReply, isErrorReply, namespacesOf, precheck, splitArgs } from './commands';
import { viewCommand } from './api';
import { quote } from './RedisPage';

describe('Redis console (step 22)', () => {
  it('splits arguments as redis-cli does', () => {
    expect(splitArgs('SET  greeting "hello world"')).toEqual(['SET', 'greeting', 'hello world']);
    expect(splitArgs(`SET k "a\\"b\\n\\x41"`)).toEqual(['SET', 'k', 'a"b\nA']);
    expect(splitArgs(`SET k 'it\\'s raw \\n'`)).toEqual(['SET', 'k', "it's raw \\n"]);
    expect(splitArgs('HSET h f ""')).toEqual(['HSET', 'h', 'f', '']);
    expect(() => splitArgs('GET "open')).toThrow(CommandError);
    expect(() => splitArgs('GET "a"b')).toThrow(CommandError);
  });

  it('pre-checks arity from the command table, leaving unknown commands to the gateway', () => {
    expect(precheck(['GET'])).toMatch(/wrong number of arguments for 'get'/);
    expect(precheck(['GET', 'k'])).toBeNull();
    expect(precheck(['SET', 'k'])).not.toBeNull();
    expect(precheck(['SET', 'k', 'v', 'EX', '10'])).toBeNull();
    expect(precheck(['HSET', 'h', 'f'])).not.toBeNull();
    expect(precheck(['NOTACOMMAND', 'x'])).toBeNull();
  });

  it('classes: FLUSHDB, FLUSHALL and SWAPDB always ask', () => {
    expect(commandClass(['get', 'k'])).toBe('read');
    expect(commandClass(['SET', 'k', 'v'])).toBe('write');
    expect(commandClass(['flushdb'])).toBe('danger');
    expect(commandClass(['SWAPDB', '0', '1'])).toBe('danger');
    expect(commandClass(['MYSTERY'])).toBe('write');
  });

  it('formats replies as redis-cli', () => {
    expect(formatReply(null)).toBe('(nil)');
    expect(formatReply(5)).toBe('(integer) 5');
    expect(formatReply(1.5)).toBe('(double) 1.5');
    expect(formatReply('OK')).toBe('"OK"');
    expect(formatReply([])).toBe('(empty array)');
    expect(formatReply(['a', ['b', 2]])).toBe('1) "a"\n2) 1) "b"\n   2) (integer) 2');
    expect(formatReply({ f: 'v', g: 'w' })).toBe('1# "f" => "v"\n2# "g" => "w"');
    expect(formatReply({ error: 'WRONGTYPE Operation' })).toBe('(error) WRONGTYPE Operation');
    expect(isErrorReply({ error: 'x' })).toBe(true);
    expect(isErrorReply({ error: 'x', other: 1 })).toBe(false);
  });

  it('finds namespaces from their __hash views and builds view commands', () => {
    expect(
      namespacesOf([
        { name: 'db0__hash', kind: 'view' },
        { name: 'db0', kind: 'table' },
        { name: 'cache__hash', kind: 'view' },
        { name: 'x__hash', kind: 'table' },
      ]),
    ).toEqual(['cache', 'db0']);
    expect(viewCommand('zset', 'z', 10)).toEqual(['ZRANGE', 'z', '0', '9', 'WITHSCORES']);
    expect(viewCommand('string', 's')).toEqual(['GET', 's']);
    expect(quote('plain:key')).toBe('plain:key');
    expect(quote('a b"c')).toBe('"a b\\"c"');
    expect(splitArgs(`TYPE ${quote('a b"c\nd')}`)).toEqual(['TYPE', 'a b"c\nd']);
  });
});
