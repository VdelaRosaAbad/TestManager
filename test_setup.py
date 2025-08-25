#!/usr/bin/env python3
"""
Script de prueba para verificar la configuración del proyecto
"""

import sys
import importlib
import subprocess
import os

def test_python_version():
    """Verifica la versión de Python"""
    print("🐍 Verificando versión de Python...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print(f"   ✅ Python {version.major}.{version.minor}.{version.micro} - OK")
        return True
    else:
        print(f"   ❌ Python {version.major}.{version.minor}.{version.micro} - Se requiere Python 3.8+")
        return False

def test_dependencies():
    """Verifica las dependencias de Python"""
    print("\n📦 Verificando dependencias de Python...")
    
    required_packages = [
        'dask',
        'pandas',
        'numpy',
        'streamlit',
        'plotly',
        'matplotlib',
        'seaborn',
        'google.cloud.bigquery',
        'google.cloud.storage',
        'pyarrow'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            if package.startswith('google.cloud.'):
                # Para paquetes de Google Cloud
                module_name = package.replace('.', '_')
                importlib.import_module(package)
                print(f"   ✅ {package} - OK")
            else:
                importlib.import_module(package)
                print(f"   ✅ {package} - OK")
        except ImportError:
            print(f"   ❌ {package} - NO INSTALADO")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n   ⚠️  Paquetes faltantes: {', '.join(missing_packages)}")
        print("   💡 Ejecuta: pip install -r requirements.txt")
        return False
    
    return True

def test_google_cloud():
    """Verifica la configuración de Google Cloud"""
    print("\n☁️ Verificando configuración de Google Cloud...")
    
    try:
        # Verificar si gcloud está disponible
        result = subprocess.run(['gcloud', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("   ✅ gcloud CLI - OK")
            version = result.stdout.split('\n')[0]
            print(f"      Versión: {version}")
        else:
            print("   ❌ gcloud CLI - NO DISPONIBLE")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("   ❌ gcloud CLI - NO INSTALADO")
        return False
    
    try:
        # Verificar configuración del proyecto
        result = subprocess.run(['gcloud', 'config', 'get-value', 'project'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            project = result.stdout.strip()
            if project:
                print(f"   ✅ Proyecto configurado: {project}")
            else:
                print("   ⚠️  No hay proyecto configurado")
                print("   💡 Ejecuta: gcloud config set project testmanager-470115")
                return False
        else:
            print("   ❌ Error obteniendo configuración del proyecto")
            return False
    except subprocess.TimeoutExpired:
        print("   ❌ Timeout obteniendo configuración del proyecto")
        return False
    
    return True

def test_dataform():
    """Verifica si Dataform está disponible"""
    print("\n⚙️ Verificando Dataform...")
    
    try:
        result = subprocess.run(['dataform', '--version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("   ✅ Dataform CLI - OK")
            version = result.stdout.strip()
            print(f"      Versión: {version}")
            return True
        else:
            print("   ❌ Dataform CLI - Error")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("   ❌ Dataform CLI - NO INSTALADO")
        print("   💡 Ejecuta: npm install -g @dataform/cli")
        return False

def test_file_structure():
    """Verifica la estructura de archivos del proyecto"""
    print("\n📁 Verificando estructura de archivos...")
    
    required_files = [
        'config.py',
        'data_processor.py',
        'dataform.yaml',
        'definitions/clean_data.sql',
        'eda_streamlit.py',
        'requirements.txt',
        'run_pipeline.sh',
        'PASOS_TECNICOS.md',
        'README.md'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"   ✅ {file_path} - OK")
        else:
            print(f"   ❌ {file_path} - NO ENCONTRADO")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n   ⚠️  Archivos faltantes: {', '.join(missing_files)}")
        return False
    
    return True

def test_config_values():
    """Verifica los valores de configuración"""
    print("\n⚙️ Verificando valores de configuración...")
    
    try:
        from config import PROJECT_ID, BUCKET, DATASET, TABLE, DATAMART_TABLE
        
        config_values = {
            'PROJECT_ID': PROJECT_ID,
            'BUCKET': BUCKET,
            'DATASET': DATASET,
            'TABLE': TABLE,
            'DATAMART_TABLE': DATAMART_TABLE
        }
        
        for key, value in config_values.items():
            if value:
                print(f"   ✅ {key}: {value}")
            else:
                print(f"   ❌ {key}: Valor vacío")
                return False
        
        return True
        
    except ImportError as e:
        print(f"   ❌ Error importando configuración: {e}")
        return False

def main():
    """Función principal de pruebas"""
    print("🧪 Iniciando pruebas de configuración del proyecto...")
    print("=" * 60)
    
    tests = [
        ("Versión de Python", test_python_version),
        ("Dependencias de Python", test_dependencies),
        ("Google Cloud", test_google_cloud),
        ("Dataform", test_dataform),
        ("Estructura de archivos", test_file_structure),
        ("Valores de configuración", test_config_values)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
            else:
                print(f"   ❌ Prueba '{test_name}' falló")
        except Exception as e:
            print(f"   ❌ Error en prueba '{test_name}': {e}")
    
    print("\n" + "=" * 60)
    print(f"📊 Resumen de pruebas: {passed}/{total} pasaron")
    
    if passed == total:
        print("🎉 ¡Todas las pruebas pasaron! El proyecto está listo para usar.")
        print("\n📋 Próximos pasos:")
        print("   1. Ejecutar: ./run_pipeline.sh")
        print("   2. O ejecutar: python data_processor.py")
        print("   3. Para EDA: streamlit run eda_streamlit.py")
    else:
        print("⚠️  Algunas pruebas fallaron. Revisa los errores arriba.")
        print("\n💡 Soluciones comunes:")
        print("   - Instalar dependencias: pip install -r requirements.txt")
        print("   - Configurar Google Cloud: gcloud config set project testmanager-470115")
        print("   - Instalar Dataform: npm install -g @dataform/cli")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
