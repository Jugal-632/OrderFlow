import base from './playwright.config.js';
export default {...base,testMatch:'phase9-evidence.spec.js',reporter:[['list'],['json',{outputFile:'../docs/evidence/phase9-browser-results.json'}]]};
