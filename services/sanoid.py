"""
Sanoid Configuration Management Service
Manages sanoid configuration for automated ZFS snapshot scheduling
"""
import subprocess
import configparser
from typing import Dict, List, Any, Optional
from pathlib import Path

from services.utils import run_privileged_command


class SanoidService:
    """Service for managing Sanoid snapshot scheduling"""
    
    SANOID_CONF = "/etc/sanoid/sanoid.conf"
    SANOID_DEFAULTS = "/etc/sanoid/sanoid.defaults.conf"
    
    def __init__(self):
        self.config_path = Path(self.SANOID_CONF)
        self.defaults_path = Path(self.SANOID_DEFAULTS)
    
    def get_config(self) -> Dict[str, Any]:
        """
        Read and parse the sanoid configuration file
        
        Returns:
            Dictionary containing all configuration sections
        """
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            result = {
                'datasets': {},
                'templates': {}
            }
            
            for section in config.sections():
                section_data = dict(config.items(section))
                
                if section.startswith('template_'):
                    result['templates'][section] = section_data
                else:
                    result['datasets'][section] = section_data
            
            return result
            
        except Exception as e:
            raise Exception(f"Failed to read sanoid configuration: {str(e)}")
    
    def get_templates(self) -> Dict[str, Dict[str, str]]:
        """
        Get all snapshot policy templates
        
        Returns:
            Dictionary of template name to template settings
        """
        try:
            config = self.get_config()
            return config.get('templates', {})
        except Exception as e:
            raise Exception(f"Failed to get templates: {str(e)}")
    
    def get_datasets(self) -> Dict[str, Dict[str, str]]:
        """
        Get all configured datasets and their policies
        
        Returns:
            Dictionary of dataset name to policy settings
        """
        try:
            config = self.get_config()
            return config.get('datasets', {})
        except Exception as e:
            raise Exception(f"Failed to get datasets: {str(e)}")
    
    def add_dataset(self, dataset_name: str, template: str, 
                   recursive: str = 'no', **kwargs) -> None:
        """
        Add a dataset to sanoid configuration
        
        Args:
            dataset_name: Name of the ZFS dataset (e.g., tank/data)
            template: Template to use (e.g., production, backup)
            recursive: Whether to include child datasets (yes/no/zfs)
            **kwargs: Additional configuration options
        """
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            # Add or update the dataset section
            if not config.has_section(dataset_name):
                config.add_section(dataset_name)
            
            config.set(dataset_name, 'use_template', template)
            config.set(dataset_name, 'recursive', recursive)
            
            # Add any additional options
            for key, value in kwargs.items():
                config.set(dataset_name, key, str(value))
            
            # Write back to file
            with open(self.config_path, 'w') as f:
                config.write(f)
                
        except Exception as e:
            raise Exception(f"Failed to add dataset: {str(e)}")
    
    def update_dataset(self, dataset_name: str, settings: Dict[str, str]) -> None:
        """
        Update settings for an existing dataset
        
        Args:
            dataset_name: Name of the dataset
            settings: Dictionary of settings to update
        """
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            if not config.has_section(dataset_name):
                raise Exception(f"Dataset {dataset_name} not found in configuration")
            
            for key, value in settings.items():
                config.set(dataset_name, key, str(value))
            
            with open(self.config_path, 'w') as f:
                config.write(f)
                
        except Exception as e:
            raise Exception(f"Failed to update dataset: {str(e)}")
    
    def remove_dataset(self, dataset_name: str) -> None:
        """
        Remove a dataset from sanoid configuration
        
        Args:
            dataset_name: Name of the dataset to remove
        """
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            if not config.has_section(dataset_name):
                raise Exception(f"Dataset {dataset_name} not found in configuration")
            
            config.remove_section(dataset_name)
            
            with open(self.config_path, 'w') as f:
                config.write(f)
                
        except Exception as e:
            raise Exception(f"Failed to remove dataset: {str(e)}")
    
    def create_template(self, template_name: str, settings: Dict[str, Any]) -> None:
        """
        Create a new snapshot policy template
        
        Args:
            template_name: Name for the template (without template_ prefix)
            settings: Dictionary of template settings
        """
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            section_name = f"template_{template_name}"
            
            if config.has_section(section_name):
                raise Exception(f"Template {template_name} already exists")
            
            config.add_section(section_name)
            
            for key, value in settings.items():
                config.set(section_name, key, str(value))
            
            with open(self.config_path, 'w') as f:
                config.write(f)
                
        except Exception as e:
            raise Exception(f"Failed to create template: {str(e)}")
    
    def update_template(self, template_name: str, settings: Dict[str, Any]) -> None:
        """
        Update an existing template
        
        Args:
            template_name: Name of the template (with or without template_ prefix)
            settings: Dictionary of settings to update
        """
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            # Handle both template_name and name formats
            section_name = template_name if template_name.startswith('template_') else f"template_{template_name}"
            
            if not config.has_section(section_name):
                raise Exception(f"Template {template_name} not found")
            
            for key, value in settings.items():
                config.set(section_name, key, str(value))
            
            with open(self.config_path, 'w') as f:
                config.write(f)
                
        except Exception as e:
            raise Exception(f"Failed to update template: {str(e)}")
    
    def delete_template(self, template_name: str) -> None:
        """
        Delete a template
        
        Args:
            template_name: Name of the template to delete
        """
        try:
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            section_name = template_name if template_name.startswith('template_') else f"template_{template_name}"
            
            if not config.has_section(section_name):
                raise Exception(f"Template {template_name} not found")
            
            config.remove_section(section_name)
            
            with open(self.config_path, 'w') as f:
                config.write(f)
                
        except Exception as e:
            raise Exception(f"Failed to delete template: {str(e)}")
    
    def run_sanoid(self, take_snapshots: bool = True, prune_snapshots: bool = False,
                   verbose: bool = False, debug: bool = False) -> Dict[str, Any]:
        """
        Run sanoid manually
        
        Args:
            take_snapshots: Take snapshots according to policy
            prune_snapshots: Prune snapshots according to policy
            verbose: Enable verbose output
            debug: Enable debug output
            
        Returns:
            Dictionary with execution results
        """
        try:
            cmd = ['sanoid']
            
            if take_snapshots:
                cmd.append('--take-snapshots')
            
            if prune_snapshots:
                cmd.append('--prune-snapshots')
            
            if verbose:
                cmd.append('--verbose')
            
            if debug:
                cmd.append('--debug')
            
            # Use run_privileged_command to handle sudo on Linux
            result = run_privileged_command(cmd, check=False)
            
            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
        except Exception as e:
            raise Exception(f"Failed to run sanoid: {str(e)}")
    
    def check_sanoid_status(self) -> Dict[str, Any]:
        """
        Check if sanoid is installed and get its status
        
        Returns:
            Dictionary with sanoid status information
        """
        try:
            # Check if sanoid is installed
            which_result = subprocess.run(
                ['which', 'sanoid'],
                capture_output=True,
                text=True
            )
            
            if which_result.returncode != 0:
                return {
                    'installed': False,
                    'path': None,
                    'version': None,
                    'config_exists': False
                }
            
            sanoid_path = which_result.stdout.strip()
            
            # Try to get version
            version_result = subprocess.run(
                ['sanoid', '--version'],
                capture_output=True,
                text=True
            )
            
            version = version_result.stdout.strip() if version_result.returncode == 0 else 'unknown'
            
            return {
                'installed': True,
                'path': sanoid_path,
                'version': version,
                'config_exists': self.config_path.exists(),
                'config_path': str(self.config_path)
            }
            
        except Exception as e:
            raise Exception(f"Failed to check sanoid status: {str(e)}")
    
    def validate_config(self) -> Dict[str, Any]:
        """
        Validate the sanoid configuration
        
        Returns:
            Dictionary with validation results
        """
        try:
            # Try to parse the config
            config = configparser.ConfigParser()
            config.read(self.config_path)
            
            errors = []
            warnings = []
            
            # Check for common issues
            dataset_sections = [s for s in config.sections() if not s.startswith('template_')]
            template_sections = [s for s in config.sections() if s.startswith('template_')]
            
            if not dataset_sections:
                warnings.append("No datasets configured")
            
            if not template_sections:
                warnings.append("No templates defined")
            
            # Check that datasets reference valid templates
            for dataset in dataset_sections:
                if config.has_option(dataset, 'use_template'):
                    templates = config.get(dataset, 'use_template').split(',')
                    for template in templates:
                        template = template.strip()
                        template_section = f"template_{template}" if not template.startswith('template_') else template
                        if template_section not in config.sections():
                            errors.append(f"Dataset '{dataset}' references non-existent template '{template}'")
            
            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'dataset_count': len(dataset_sections),
                'template_count': len(template_sections)
            }
            
        except Exception as e:
            return {
                'valid': False,
                'errors': [f"Failed to parse configuration: {str(e)}"],
                'warnings': [],
                'dataset_count': 0,
                'template_count': 0
            }
