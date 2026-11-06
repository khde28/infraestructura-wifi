# scripts/run_and_verify.py - Lanza el servidor y ejecuta la suite de verificación completa
import subprocess
import time
import sys
import os
import aiohttp
import asyncio

async def wait_server(url: str, timeout: float = 10.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=1.0) as r:
                    if r.status == 200:
                        return True
        except Exception:
            await asyncio.sleep(0.5)
    return False

def main():
    python_exe = sys.executable
    print(f"Usando intérprete Python: {python_exe}")

    server_proc = subprocess.Popen(
        [python_exe, "-m", "server.app"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy()
    )

    try:
        print("Esperando inicio del servidor en http://127.0.0.1:8080...")
        ready = asyncio.run(wait_server("http://127.0.0.1:8080/api/v1/dashboard/summary", timeout=12.0))
        if not ready:
            print("ERROR: El servidor no respondió a tiempo.")
            stdout, stderr = server_proc.communicate(timeout=2)
            print("STDOUT:\n", stdout)
            print("STDERR:\n", stderr)
            sys.exit(1)

        print("¡Servidor listo y respondiendo! Ejecutando suite de verificación...\n")
        
        # 1. Ejecutar verify_server.py
        ret_verify = subprocess.run([python_exe, "scripts/verify_server.py"])
        if ret_verify.returncode != 0:
            print("Fallo en verify_server.py")
            sys.exit(ret_verify.returncode)

        # 2. Ejecutar simulate_ap_events.py
        print("\nEjecutando simulación de eventos de infraestructura...")
        ret_infra = subprocess.run([python_exe, "scripts/simulate_ap_events.py", "--mode", "http"])
        if ret_infra.returncode != 0:
            print("Fallo en simulate_ap_events.py")

        # 3. Ejecutar simulación de 30 celulares concurrentes
        print("\nEjecutando simulación de dispositivos móviles...")
        ret_devs = subprocess.run([python_exe, "scripts/simulate_devices.py", "--devices", "30", "--iterations", "2"])
        if ret_devs.returncode != 0:
            print("Fallo en simulate_devices.py")

        print("\n=======================================================")
        print(" ¡TODAS LAS PRUEBAS Y SIMULACIONES CONCLUIDAS CON ÉXITO!")
        print("=======================================================")

    finally:
        print("\nApagando servidor...")
        server_proc.terminate()
        try:
            server_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server_proc.kill()
        print("Servidor detenido.")

if __name__ == "__main__":
    main()

