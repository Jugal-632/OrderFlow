import {defineConfig} from '@playwright/test';
export default defineConfig({testDir:'./tests/browser',workers:1,use:{baseURL:'http://127.0.0.1:5173',channel:'chrome',headless:true},webServer:{command:'node server.mjs',url:'http://127.0.0.1:5173',reuseExistingServer:false},reporter:[['list'],['json',{outputFile:'../docs/evidence/phase8-browser-results.json'}]]});
