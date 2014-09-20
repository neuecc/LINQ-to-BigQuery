using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigQuery.Linq
{
    public static partial class BqFunc
    {
        /// <summary>
        /// Given a URL, returns the host name as a string.
        /// <para>Example: HOST('http://www.google.com:80/index.html') returns 'www.google.com'</para>
        /// </summary>
        [FunctionName("HOST")]
        public static string Host(string url)
        {
            throw Invalid();
        }

        /// <summary>
        /// Given a URL, returns the domain as a string.
        /// <para>Example: DOMAIN('http://www.google.com:80/index.html') returns 'google.com'</para>
        /// </summary>
        [FunctionName("DOMAIN")]
        public static string Domain(string url)
        {
            throw Invalid();
        }

        /// <summary>
        /// Given a URL, returns the top level domain plus any country domain in the URL.
        /// <para>Example: TLD('http://www.google.com:80/index.html') returns '.com'. TLD('http://www.google.co.uk:80/index.html') returns '.co.uk'.</para>
        /// </summary>
        [FunctionName("TLD")]
        public static string Tld(string url)
        {
            throw Invalid();
        }
    }
}
