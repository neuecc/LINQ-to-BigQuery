using System;
using System.Linq;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace BigQuery.Linq
{
    internal interface IReflectionAccessor
    {
        object GetValue(object source);
    }

    internal static class ReflectionAccessor
    {
        public static IReflectionAccessor Create(MemberInfo memberInfo)
        {
            var propInfo = memberInfo as PropertyInfo;
            if (propInfo != null)
            {
                return new PropertyInfoAccessor(propInfo);
            }

            var fieldInfo = memberInfo as FieldInfo;
            if (fieldInfo != null)
            {
                return new FieldInfoAccessor(fieldInfo);
            }

            throw new ArgumentException("invalid member info:" + memberInfo.GetType());
        }

        class PropertyInfoAccessor : IReflectionAccessor
        {
            readonly MethodInfo methodInfo;

            public PropertyInfoAccessor(PropertyInfo propInfo)
            {
                methodInfo = propInfo.GetGetMethod();
            }

            public object GetValue(object source)
            {
                return methodInfo.Invoke(source, null);
            }
        }

        class FieldInfoAccessor : IReflectionAccessor
        {
            readonly FieldInfo fieldInfo;

            public FieldInfoAccessor(FieldInfo fieldInfo)
            {
                this.fieldInfo = fieldInfo;
            }

            public object GetValue(object source)
            {
                return fieldInfo.GetValue(source);
            }
        }
    }

    internal static class ExpressionHelper
    {
        public static object GetValue(Expression expression)
        {
            if (expression is ConstantExpression) return ((ConstantExpression)expression).Value;
            if (expression is NewExpression)
            {
                var expr = (NewExpression)expression;
                var parameters = expr.Arguments.Select(x => GetValue(x)).ToArray();

                return Activator.CreateInstance(expr.Constructor.DeclaringType);
            }

            var memberExpressions = new List<MemberExpression>();
            while (!(expression is ConstantExpression))
            {
                if ((expression is UnaryExpression) && (expression.NodeType == ExpressionType.Convert))
                {
                    expression = ((UnaryExpression)expression).Operand;
                    continue;
                }

                var memberExpression = (MemberExpression)expression;
                memberExpressions.Add(memberExpression);
                var nextExpression = memberExpression.Expression;
                if (nextExpression == null) break;
                expression = nextExpression;
            }

            var rootExpression = expression as ConstantExpression;
            var value = (rootExpression != null)
                    ? rootExpression.Value
                    : null;

            for (int i = memberExpressions.Count - 1; i >= 0; i--)
            {
                var expr = memberExpressions[i];

                var accessor = ReflectionAccessor.Create(expr.Member);
                value = accessor.GetValue(value);
                if (value == null) return null; // avoid null exception.
            }

            return value;
        }
    }
}