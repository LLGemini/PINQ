using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using PINQ;
using System.IO;

namespace TestHarness
{
    public class TestHarness
    {
        public class PINQAgentLogger : PINQAgent
        {
            string name;
            double total;
            public override bool apply(double epsilon)
            {
                total += epsilon;
                Console.WriteLine("**privacy change**\tdelta: " + epsilon.ToString("0.00") + "\ttotal: " + total.ToString("0.00") + "\t(" + name + ")");
                Console.WriteLine("**privacy change**\tdelta: {0:F2}\ttotal: {1:F2}\t({2})", epsilon, total, name);

                return true;
            }

            public PINQAgentLogger(string n) { name = n; }
        }

        static void Main(string[] args)
        {
            // preparing a private data source
            var filename = @"..\..\TestHarness.cs";
            var data = File.ReadAllLines(filename).AsQueryable();
            var text = new PINQueryable<string>(data, new PINQAgentLogger(filename));

            /**** Data is now sealed up. Use from this point on is unrestricted ****/

            // output a noisy count of the number of lines of text
            Console.WriteLine("Lines of text: " + text.NoisyCount(1.0));

            // restrict using a user defined predicate, and count again (with noise)
            Console.WriteLine("Lines with semi-colons: " + text.Where(line => line.Contains(';')).NoisyCount(1.0));

            // think about splitting the records into arrays (declarative, so nothing happens yet)
            var words = text.Select(line => line.Split(' '));

            // partition the data by number of "words", and count how many of each type there are
            var keys = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            var parts = words.Partition(keys, line => line.Count());
            foreach (var count in keys)
                Console.WriteLine("Lines with " + count + " words:" + "\t" + parts[count].NoisyCount(0.1));

            Console.ReadKey();
        }
    }
}
