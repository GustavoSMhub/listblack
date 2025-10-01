# -*- mode: python ; coding: utf-8 -*-

import os

# Bloque de análisis: El cerebro de la configuración
a = Analysis(
    ['search_app/app.py'],  # Punto de entrada principal de tu aplicación
    pathex=['.'],  # Añade el directorio raíz a la ruta de búsqueda de Python
    binaries=[],
    datas=[
        ('search_app/templates', 'search_app/templates'),  # Incluye la carpeta de plantillas de Flask
        ('.env', '.'),  # Incluye el archivo .env que crea el workflow
        ('data', 'data'),  # Incluye tu carpeta de datos
        ('sql', 'sql')    # Incluye tu carpeta de archivos sql
    ],
    hiddenimports=[],  # Nombres de librerías que PyInstaller podría no encontrar
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

# Bloque PYZ: Empaqueta las dependencias de Python
pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# Bloque EXE: Define el archivo ejecutable final
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='busqueda_unificada_win',  # El nombre de tu archivo .exe
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # True para ver la consola (útil para depurar), False para ocultarla
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# Bloque COLLECT: Agrupa todo en una carpeta final (no necesita cambios)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='busqueda_unificada_win',
)