import { IDType } from '../../interfaces.js';

export const lowerCaseHexChars = Object.freeze([
    'a',
    'b',
    'c',
    'd',
    'e',
    'f'
]);

export const upperCaseHexChars = Object.freeze([
    'A',
    'B',
    'C',
    'D',
    'E',
    'F'
]);

export const lowerCaseChars = Object.freeze([
    ...lowerCaseHexChars,
    'g',
    'h',
    'i',
    'j',
    'k',
    'l',
    'm',
    'n',
    'o',
    'p',
    'q',
    'r',
    's',
    't',
    'u',
    'v',
    'w',
    'x',
    'y',
    'z',
]);

export const upperCaseChars = Object.freeze([
    ...upperCaseHexChars,
    'G',
    'H',
    'I',
    'J',
    'K',
    'L',
    'M',
    'N',
    'O',
    'P',
    'Q',
    'R',
    'S',
    'T',
    'U',
    'V',
    'W',
    'X',
    'Y',
    'Z',
]);

export const numerics = Object.freeze([
    '0',
    '1',
    '2',
    '3',
    '4',
    '5',
    '6',
    '7',
    '8',
    '9',
]);

export const base64URLSpecialChars = Object.freeze([
    '-',
    '_'
]);

export const base64SpecialChars = Object.freeze([
    ...base64URLSpecialChars,
    '\\+',
    '/'
]);

// base64 based: upper, lower, numbers then special chars
export const base64url = Object.freeze([
    ...upperCaseChars,
    ...lowerCaseChars,
    ...numerics,
    ...base64URLSpecialChars
]);

export const base64 = Object.freeze([
    ...upperCaseChars,
    ...lowerCaseChars,
    ...numerics,
    ...base64SpecialChars
]);

// hexadecimal: numbers first then chars
export const hexadecimal = Object.freeze([
    ...numerics,
    ...lowerCaseHexChars
]);

export const HEXADECIMAL = Object.freeze([
    ...numerics,
    ...upperCaseHexChars
]);

export function getKeyArray(keyType: IDType): readonly string[] {
    if (keyType === IDType.base64url) return base64url;
    if (keyType === IDType.base64) return base64;
    if (keyType === IDType.hexadecimal) return hexadecimal;
    if (keyType === IDType.HEXADECIMAL) return HEXADECIMAL;

    throw new Error('Could not find correct key type');
}
