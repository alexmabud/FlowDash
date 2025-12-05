import sys
import os
import importlib

# Add project root to path
sys.path.append(os.getcwd())

modules_to_check = [
    "flowdash_pages.dashboard.dashboard",
    "flowdash_pages.metas.metas"
]

for module_name in modules_to_check:
    try:
        print(f"\nAttempting to import {module_name}...")
        mod = importlib.import_module(module_name)
        print(f"Module imported: {mod}")
        print(f"File: {getattr(mod, '__file__', 'unknown')}")
        
        print("Checking attributes:")
        has_render = hasattr(mod, "render")
        has_main = hasattr(mod, "main")
        has_app = hasattr(mod, "app")
        
        print(f"Has render: {has_render}")
        if has_render:
            print(f"render is callable: {callable(mod.render)}")
            
        print(f"Has main: {has_main}")
        print(f"Has app: {has_app}")
        
    except Exception as e:
        print(f"ERROR importing {module_name}: {e}")
        import traceback
        traceback.print_exc()
