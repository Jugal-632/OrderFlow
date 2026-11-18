"""Inspect only built runtime images; never copy host build output into containers."""
import hashlib,json,os,subprocess,tempfile,zipfile
from pathlib import Path
root=Path(__file__).resolve().parent.parent
out=root/'docs/evidence/phase10';out.mkdir(parents=True,exist_ok=True)
reports=[]
for service in ['auth-service','order-service','inventory-service','notification-service','api-gateway']:
    image=f"orderflow/{service}:{os.getenv('IMAGE_TAG','local')}"
    metadata=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))[0]
    assert metadata['Config']['User']=='10001:10001'
    container=subprocess.check_output(['docker','create',image],text=True).strip()
    try:
        with tempfile.TemporaryDirectory() as directory:
            jar=Path(directory)/'app.jar'
            subprocess.run(['docker','cp',container+':/app/app.jar',str(jar)],check=True,capture_output=True)
            with zipfile.ZipFile(jar) as archive:
                classes=[name for name in archive.namelist() if name.startswith('BOOT-INF/classes/')]
                assert not any('CrashProcess' in name or 'Phase9SystemIT' in name or name.endswith('Test.class') for name in classes)
                config=archive.read('BOOT-INF/classes/application.yml').decode()
                assert 'password: password' not in config
                assert 'local-development-only' not in config
            reports.append({'service':service,'imageId':metadata['Id'],'user':metadata['Config']['User'],'jarSha256':hashlib.sha256(jar.read_bytes()).hexdigest(),'testFailpointsAbsent':True,'embeddedLocalCredentialsAbsent':True})
    finally:subprocess.run(['docker','rm',container],check=True,capture_output=True)
(out/'runtime-artifacts.json').write_text(json.dumps(reports,indent=2))
print('PASS: five runtime images, non-root user, no test failpoints or embedded local credentials')
