#!/usr/bin/env python3
"""
Script universel pour exécuter différents types de scripts Python
Supporte : API, ETL (Neo4j, MySQL, etc.), Scripts de traitement, etc.
"""

import os
import sys
import argparse
import logging
import subprocess
import json
import time
from pathlib import Path
from datetime import datetime

class ScriptRunner:
    def __init__(self):
        self.setup_logging()
        self.config_dir = Path(os.getenv('CONFIG_DIR', './config'))
        self.scripts_dir = Path('./scripts')
        self.output_dir = Path(os.getenv('OUTPUT_DIR', './output'))
        
        # Créer les répertoires s'ils n'existent pas
        self.output_dir.mkdir(exist_ok=True)
        self.config_dir.mkdir(exist_ok=True)
        
    def setup_logging(self):
        """Configuration du logging"""
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('/app/logs/run-all.log')
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_config(self, script_type, script_name):
        """Charger la configuration pour un script spécifique"""
        config_file = self.config_dir / f"{script_type}_{script_name}.json"
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                self.logger.info(f"Configuration chargée: {config_file}")
                return config
            except Exception as e:
                self.logger.warning(f"Erreur lecture config {config_file}: {e}")
        
        return self.get_default_config(script_type)

    def get_default_config(self, script_type):
        """Configuration par défaut selon le type de script"""
        configs = {
            'api': {
                'host': '0.0.0.0',
                'port': 8000,
                'debug': False,
                'workers': 1
            },
            'etl': {
                'max_retries': 5,
                'retry_delay': 10,
                'batch_size': 1000,
                'output_format': 'csv'
            },
            'neo4j': {
                'uri': os.getenv('NEO4J_URI', 'bolt://neo4j:7687'),
                'user': os.getenv('NEO4J_USER', 'neo4j'),
                'password': os.getenv('NEO4J_PASSWORD', '12345678'),
                'hdfs_url': os.getenv('HDFS_URL', 'http://namenode:9870')
            },
            'mysql': {
                'host': os.getenv('DB_HOST', 'mysql'),
                'port': int(os.getenv('DB_PORT', '3306')),
                'user': os.getenv('DB_USER', 'root'),
                'password': os.getenv('DB_PASSWORD', '1234'),
                'database': os.getenv('DB_NAME', 'education')
            }
        }
        return configs.get(script_type, {})

    def setup_environment(self, config):
        """Configurer les variables d'environnement"""
        for key, value in config.items():
            if isinstance(value, (str, int, float, bool)):
                os.environ[str(key).upper()] = str(value)
                self.logger.debug(f"ENV: {key.upper()} = {value}")

    def run_api_script(self, script_name, config):
        """Exécuter un script API"""
        script_path = self.scripts_dir / 'api' / f"{script_name}.py"
        if not script_path.exists():
            raise FileNotFoundError(f"Script API non trouvé: {script_path}")
        
        self.logger.info(f"🚀 Démarrage de l'API: {script_name}")
        
        cmd = [
            sys.executable, 
            str(script_path),
            '--host', config.get('host', '0.0.0.0'),
            '--port', str(config.get('port', 8000))
        ]
        
        if config.get('debug'):
            cmd.append('--debug')
        
        return subprocess.run(cmd)

    def run_etl_script(self, script_name, config):
        """Exécuter un script ETL"""
        script_path = self.scripts_dir / 'etl' / f"{script_name}.py"
        if not script_path.exists():
            raise FileNotFoundError(f"Script ETL non trouvé: {script_path}")
        
        self.logger.info(f"📊 Démarrage ETL: {script_name}")
        
        # Configurer l'environnement
        self.setup_environment(config)
        
        # Exécuter le script
        return subprocess.run([sys.executable, str(script_path)])

    def run_generic_script(self, script_type, script_name, config):
        """Exécuter un script générique"""
        script_path = self.scripts_dir / script_type / f"{script_name}.py"
        if not script_path.exists():
            # Essayer dans le répertoire racine des scripts
            script_path = self.scripts_dir / f"{script_name}.py"
            if not script_path.exists():
                raise FileNotFoundError(f"Script non trouvé: {script_name}")
        
        self.logger.info(f"⚙️  Démarrage script {script_type}: {script_name}")
        
        # Configurer l'environnement
        self.setup_environment(config)
        
        # Exécuter le script
        return subprocess.run([sys.executable, str(script_path)])

    def list_available_scripts(self):
        """Lister les scripts disponibles"""
        self.logger.info("📋 Scripts disponibles:")
        
        for script_type_dir in self.scripts_dir.iterdir():
            if script_type_dir.is_dir():
                scripts = list(script_type_dir.glob('*.py'))
                if scripts:
                    self.logger.info(f"  {script_type_dir.name}:")
                    for script in scripts:
                        self.logger.info(f"    - {script.stem}")
        
        # Scripts à la racine
        root_scripts = list(self.scripts_dir.glob('*.py'))
        if root_scripts:
            self.logger.info("  racine:")
            for script in root_scripts:
                self.logger.info(f"    - {script.stem}")

    def run_script(self, script_type, script_name, config_override=None):
        """Point d'entrée principal pour exécuter un script"""
        try:
            self.logger.info("="*60)
            self.logger.info(f"🏃 EXÉCUTION: {script_type}/{script_name}")
            self.logger.info("="*60)
            
            # Charger la configuration
            config = self.load_config(script_type, script_name)
            if config_override:
                config.update(config_override)
            
            self.logger.info(f"📋 Configuration: {config}")
            
            # Exécuter selon le type
            if script_type == 'api':
                result = self.run_api_script(script_name, config)
            elif script_type == 'etl':
                result = self.run_etl_script(script_name, config)
            else:
                result = self.run_generic_script(script_type, script_name, config)
            
            if result.returncode == 0:
                self.logger.info("✅ Script terminé avec succès")
            else:
                self.logger.error(f"❌ Script échoué avec le code: {result.returncode}")
            
            return result.returncode
            
        except FileNotFoundError as e:
            self.logger.error(f"❌ Fichier non trouvé: {e}")
            return 1
        except Exception as e:
            self.logger.error(f"❌ Erreur inattendue: {e}")
            return 1

def main():
    parser = argparse.ArgumentParser(
        description='Script universel pour exécuter des scripts Python',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python run-all.py --type api --name main
  python run-all.py --type etl --name neo4j-extraction
  python run-all.py --type mysql --name data-import
  python run-all.py --list
        """
    )
    
    parser.add_argument('--type', '-t', 
                       help='Type de script (api, etl, neo4j, mysql, etc.)')
    parser.add_argument('--name', '-n', 
                       help='Nom du script à exécuter')
    parser.add_argument('--config', '-c', 
                       help='Fichier de configuration JSON personnalisé')
    parser.add_argument('--list', '-l', action='store_true',
                       help='Lister les scripts disponibles')
    parser.add_argument('--env', action='append',
                       help='Variables d\'environnement (format: KEY=VALUE)')
    
    # Support des variables d'environnement
    script_type = os.getenv('SCRIPT_TYPE')
    script_name = os.getenv('SCRIPT_NAME')
    
    args = parser.parse_args()
    
    # Priorité: args CLI > variables d'environnement
    script_type = args.type or script_type
    script_name = args.name or script_name
    
    runner = ScriptRunner()
    
    # Lister les scripts si demandé
    if args.list:
        runner.list_available_scripts()
        return 0
    
    # Vérifier les paramètres requis
    if not script_type or not script_name:
        parser.print_help()
        print("\n❌ Erreur: --type et --name sont requis")
        print("💡 Ou utilisez les variables d'environnement SCRIPT_TYPE et SCRIPT_NAME")
        return 1
    
    # Variables d'environnement personnalisées
    config_override = {}
    if args.env:
        for env_var in args.env:
            if '=' in env_var:
                key, value = env_var.split('=', 1)
                config_override[key] = value
                os.environ[key] = value
    
    # Configuration personnalisée
    if args.config:
        try:
            with open(args.config, 'r') as f:
                custom_config = json.load(f)
                config_override.update(custom_config)
        except Exception as e:
            runner.logger.error(f"❌ Erreur lecture config {args.config}: {e}")
            return 1
    
    # Exécuter le script
    return runner.run_script(script_type, script_name, config_override)

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)