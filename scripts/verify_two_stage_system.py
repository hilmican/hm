#!/usr/bin/env python3
"""
Verification script for two-stage Agent/Serializer system.
Checks that all components are in place and working.
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def check_file_exists(path: str, description: str) -> bool:
    """Check if a file exists."""
    full_path = project_root / path
    exists = full_path.exists()
    status = "✅" if exists else "❌"
    print(f"{status} {description}: {path}")
    if not exists:
        print(f"   Missing file: {full_path}")
    return exists

def check_function_exists(module_path: str, function_name: str, description: str) -> bool:
    """Check if a function exists in a module."""
    try:
        module_parts = module_path.split(".")
        module = __import__(module_path, fromlist=[module_parts[-1]])
        func = getattr(module, function_name, None)
        exists = callable(func)
        status = "✅" if exists else "❌"
        print(f"{status} {description}: {module_path}.{function_name}()")
        return exists
    except Exception as e:
        print(f"❌ {description}: {module_path}.{function_name}() - Error: {e}")
        return False

def check_prompt_loading() -> bool:
    """Check if prompts can be loaded."""
    try:
        from app.services.prompts import get_serializer_prompt, get_global_system_prompt
        
        serializer = get_serializer_prompt()
        global_prompt = get_global_system_prompt()
        
        has_serializer = bool(serializer and len(serializer.strip()) > 0)
        has_global = bool(global_prompt and len(global_prompt.strip()) > 0)
        
        status_ser = "✅" if has_serializer else "❌"
        status_glob = "✅" if has_global else "❌"
        
        print(f"{status_ser} Serializer prompt loading: {len(serializer)} chars")
        print(f"{status_glob} Global system prompt loading: {len(global_prompt)} chars")
        
        return has_serializer and has_global
    except Exception as e:
        print(f"❌ Prompt loading failed: {e}")
        return False

def check_ai_client_methods() -> bool:
    """Check if AI client has required methods."""
    try:
        from app.services.ai import AIClient
        
        client = AIClient(model="gpt-4o-mini")
        
        has_generate_chat = hasattr(client, "generate_chat") and callable(getattr(client, "generate_chat"))
        has_generate_json = hasattr(client, "generate_json") and callable(getattr(client, "generate_json"))
        
        status_chat = "✅" if has_generate_chat else "❌"
        status_json = "✅" if has_generate_json else "❌"
        
        print(f"{status_chat} AI client has generate_chat() method")
        print(f"{status_json} AI client has generate_json() method")
        
        return has_generate_chat and has_generate_json
    except Exception as e:
        print(f"❌ AI client check failed: {e}")
        return False

def check_tool_schemas() -> bool:
    """Check if new tool schemas are defined in draft_reply."""
    try:
        import inspect
        from app.services.ai_reply import draft_reply
        
        source = inspect.getsource(draft_reply)
        
        tools_to_check = [
            "change_focus_product",
            "add_cart_item",
            "analyze_customer_image",
            "send_product_image_to_customer"
        ]
        
        all_found = True
        for tool in tools_to_check:
            found = tool in source
            status = "✅" if found else "❌"
            print(f"{status} Tool schema: {tool}")
            if not found:
                all_found = False
        
        return all_found
    except Exception as e:
        print(f"❌ Tool schema check failed: {e}")
        return False

def check_draft_reply_structure() -> bool:
    """Check if draft_reply uses two-stage flow."""
    try:
        import inspect
        from app.services.ai_reply import draft_reply
        
        source = inspect.getsource(draft_reply)
        
        indicators = [
            ("generate_chat", "Agent stage call"),
            ("generate_json", "Serializer stage call"),
            ("agent_reply_text", "Agent output variable"),
            ("serializer_prompt", "Serializer prompt variable"),
            ("get_serializer_prompt", "Serializer prompt loader"),
        ]
        
        all_found = True
        for indicator, description in indicators:
            found = indicator in source
            status = "✅" if found else "❌"
            print(f"{status} Two-stage indicator: {description}")
            if not found:
                all_found = False
        
        return all_found
    except Exception as e:
        print(f"❌ draft_reply structure check failed: {e}")
        return False

def main():
    """Run all verification checks."""
    print("=" * 60)
    print("Two-Stage Agent/Serializer System Verification")
    print("=" * 60)
    print()
    
    results = []
    
    print("📁 File Existence Checks")
    print("-" * 60)
    results.append(check_file_exists(
        "app/services/ai_reply.py",
        "Main AI reply service"
    ))
    results.append(check_file_exists(
        "app/services/ai.py",
        "AI client service"
    ))
    results.append(check_file_exists(
        "app/services/prompts.py",
        "Prompts loader"
    ))
    results.append(check_file_exists(
        "app/services/prompts/AGENT_SERIALIZER_PROMPT.txt",
        "Serializer prompt file"
    ))
    results.append(check_file_exists(
        "app/services/prompts/REVISED_GLOBAL_SYSTEM_PROMPT.txt",
        "Agent system prompt file"
    ))
    results.append(check_file_exists(
        "scripts/worker_reply.py",
        "Worker script"
    ))
    print()
    
    print("🔧 Function Existence Checks")
    print("-" * 60)
    results.append(check_function_exists(
        "app.services.prompts",
        "get_serializer_prompt",
        "Serializer prompt loader"
    ))
    results.append(check_function_exists(
        "app.services.prompts",
        "get_global_system_prompt",
        "Global system prompt loader"
    ))
    results.append(check_function_exists(
        "app.services.ai_reply",
        "draft_reply",
        "Main draft reply function"
    ))
    print()
    
    print("🤖 AI Client Checks")
    print("-" * 60)
    results.append(check_ai_client_methods())
    print()
    
    print("📝 Prompt Loading Checks")
    print("-" * 60)
    results.append(check_prompt_loading())
    print()
    
    print("🛠️  Tool Schema Checks")
    print("-" * 60)
    results.append(check_tool_schemas())
    print()
    
    print("🏗️  Two-Stage Structure Checks")
    print("-" * 60)
    results.append(check_draft_reply_structure())
    print()
    
    # Summary
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} checks passed")
    print("=" * 60)
    
    if passed == total:
        print("✅ All checks passed! System is ready.")
        return 0
    else:
        print("❌ Some checks failed. Please review above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

