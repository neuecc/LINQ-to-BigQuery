using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    internal static class NameConverter
    {
        readonly static HashSet<string> ReservedIdentifiers = new HashSet<string>
        {
            "abstract",
            "as",
            "base",
            "bool",
            "break",
            "byte",
            "case",
            "catch",
            "char",
            "checked",
            "class",
            "const",
            "continue",
            "decimal",
            "default",
            "delegate",
            "do",
            "double",
            "else",
            "enum",
            "event",
            "explicit",
            "extern",
            "false",
            "finally",
            "fixed",
            "float",
            "for",
            "foreach",
            "goto",
            "if",
            "implicit",
            "in",
            "in",
            "int",
            "interface",
            "internal",
            "is",
            "lock",
            "long",
            "namespace",
            "new",
            "null",
            "object",
            "operator",
            "out",
            "out",
            "override",
            "params",
            "private",
            "protected",
            "public",
            "readonly",
            "ref",
            "return",
            "sbyte",
            "sealed",
            "short",
            "sizeof",
            "stackalloc",
            "static",
            "string",
            "struct",
            "switch",
            "this",
            "throw",
            "true",
            "try",
            "typeof",
            "uint",
            "ulong",
            "unchecked",
            "unsafe",
            "ushort",
            "using",
            "virtual",
            "void",
            "volatile",
            "while",
        };


        public static string ConvertSafeName(string name)
        {
            if (Regex.IsMatch(name, "^[0123456789]"))
            {
                name = "_" + name;
            }
            if (ReservedIdentifiers.Contains(name))
            {
                name = "@" + name;
            }

            return name.Replace(" ", "_").Replace("-", "_").Replace(":", "_");
        }
    }
}