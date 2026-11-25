import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../',import.meta.url));
const manifest = JSON.parse(await readFile(join(root,'package.json'),'utf8'));
const dir = await mkdtemp(join(tmpdir(),'watch-pack-'));
try {
  const app = join(dir,'consumer');
  await mkdir(app);
  await writeFile(join(app,'package.json'),'{"name":"fixture-consumer","private":true,"type":"module"}');
  await writeFile(join(dir,'user.npmrc'),'');
  await writeFile(join(dir,'global.npmrc'),'');
  const env = {PATH:process.env.PATH,npm_config_offline:'true',npm_config_ignore_scripts:'true',
    npm_config_audit:'false',npm_config_fund:'false',npm_config_cache:join(dir,'cache'),
    npm_config_userconfig:join(dir,'user.npmrc'),npm_config_globalconfig:join(dir,'global.npmrc')};
  const run = (command,args,cwd) => execFileSync(command,args,{cwd,env,encoding:'utf8',timeout:60000});
  const [tar] = JSON.parse(run('npm',['pack','--json','--pack-destination',dir],root));
  const actual = tar.files.map(f => f.path).sort();
  assert.deepEqual(actual,[...manifest.files,'LICENSE','README.md','package.json'].sort());
  assert.equal(manifest.license,'MIT');
  assert.equal(manifest.private,true);
  assert.equal(Object.keys(manifest.dependencies ?? {}).length,0);
  assert.equal(tar.bundled.length,0);
  run('npm',['install','--no-save','--no-package-lock',join(dir,tar.filename)],app);
  run(process.execPath,['--input-type=module','-e',`
    import assert from 'node:assert/strict';
    import {WatchStore,WatchRuntime,Inbox} from 'agent-watch-runtime';
    import {normalizeConfig,normalizePatch,configSummary} from 'agent-watch-runtime/configuration';
    import {validatePredicate} from 'agent-watch-runtime/sdk';
    import {syntheticSource} from 'agent-watch-runtime/sources';
    import {serveStdio} from 'agent-watch-runtime/mcp';
    import {deliverOnce} from 'agent-watch-runtime/delivery';
    for (const fn of [WatchStore,WatchRuntime,Inbox,syntheticSource,serveStdio,deliverOnce]) assert.equal(typeof fn,'function');
    assert.equal(configSummary(normalizeConfig({version:1,mode:'changes',source_id:'fixture',consumer_id:'worker'})).version,1);
    assert.deepEqual(normalizePatch({version:1,delivery:{enabled:false}}).delivery,{enabled:false});
    assert.deepEqual(validatePredicate({path:'',equals:true}),{path:'',equals:true});
  `],app);
  const executable = join(app,'node_modules','.bin','agent-watch');
  const init = JSON.parse(run(executable,['init',join(dir,'state')],app));
  assert.equal(init.state,'initialized');
  const installed = join(app,'node_modules','agent-watch-runtime');
  run(process.execPath,['scripts/check-static.mjs','--package'],installed);
  const input = [
    {jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25',capabilities:{},clientInfo:{name:'fixture',version:'1'}}},
    {jsonrpc:'2.0',method:'notifications/initialized'},
    {jsonrpc:'2.0',id:2,method:'tools/list',params:{}},
  ].map(message => JSON.stringify(message)).join('\n') + '\n';
  const replies = execFileSync(process.execPath,[join(installed,'bin','watch.mjs'),'serve',join(dir,'state')],
    {cwd:app,env,input,encoding:'utf8',timeout:15000}).trim().split('\n').map(line => JSON.parse(line));
  assert.equal(replies.find(message => message.id === 1)?.result?.protocolVersion,'2025-11-25');
  assert.ok(replies.find(message => message.id === 2)?.result?.tools.some(tool => tool.name === 'check_now'));
  const demo = JSON.parse(run(process.execPath,['examples/demo.mjs'],installed));
  assert.deepEqual([demo.sequence,demo.accepted,demo.processed],[1,1,1]);
  console.log(JSON.stringify({package:manifest.name,files:actual.length,integrity:tar.integrity,
    install:'offline',accepted:demo.accepted,processed:demo.processed}));
} finally {
  await rm(dir,{recursive:true,force:true});
}
