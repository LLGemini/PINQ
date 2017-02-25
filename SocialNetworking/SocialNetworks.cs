using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using PINQ;

namespace SocialNetworks
{
    class Program
    {
        public static IEnumerable<int[]> GenerateData(int participants)
        {
            var RNG = new Random();
            while (true)
                yield return new int[] { RNG.Next() % participants, RNG.Next() % participants };
        }

        public static PINQueryable<int[]> BoundDegree(PINQueryable<int[]> edges, int bound)
        {
            // reduce the degree of the graph
            var clamped = edges.GroupBy(edge => edge[0])                        // collect up edges by source
                               .SelectMany(bound, group => group.Take(bound))   // only keep *bound* of them
                               .GroupBy(edge => edge[1])                        // collect up edges by target
                               .SelectMany(bound, group => group.Take(bound));  // only keep *bound* of them

            // A more "privacy efficient" approach uses the generalized Distinct transformation. 
            // The stability constant here is 4 instead of 4 * bound^2 using the GroupBy operations above.
            // clamped = edges.Distinct(bound, edge => edge[0])
            //                .Distinct(bound, edge => edge[1])

            // symmetrize (if interested) and return. degree is now at most 2 * bound.
            return clamped.Select(x => new int[] { x[1], x[0] })
                          .Concat(clamped)
                          .Distinct();
        }

        // entending paths in a degree-bounded graph. rather, extending paths in a degree-bounded way; whether the graph is or is not is irrelevant.
        public static PINQueryable<int[]> ExtendPaths(PINQueryable<int[]> pathsA, PINQueryable<int[]> pathsB, int boundA, int boundB)
        {
            // just a bounded join
            return pathsA.Join(pathsB,
                               pathA => pathA.Last(),                                    // the last node in the first path
                               pathB => pathB.First(),                                   // the first node in the last path
                               boundA,                                                   // the number of paths that end at any one endpoint is at most boundA.
                               boundB,                                                   // the number of paths that start from any one endpoint is at most boundB.
                               (pathA, pathB) => pathA.Concat(pathB.Skip(1)).ToArray()); // stick the two paths together
        }

        static void Main(string[] args)
        {
            var participants = 1000;
            var edges = 10000;

            var sourcegraph = GenerateData(participants).Take(edges).ToArray().AsQueryable();
            var securegraph = new PINQueryable<int[]>(sourcegraph, null);

            // we'll start by computing degree distributions
            var nodes = securegraph.GroupBy(x => x[0]);

            var nodeparts = nodes.Partition(Enumerable.Range(0, 20).ToArray(), x => x.Count());
            foreach (var degree in Enumerable.Range(0, 20))
                Console.WriteLine("degree {0}:\t{1:F2}\t+/- {2:F2}", degree, nodeparts[degree].NoisyCount(0.1), 10.0);

            Console.WriteLine();


            // for a buch of the analyses, we want the degree to be bounded
            var bound = 10;
            var bounded = BoundDegree(securegraph, bound).Materialize();


            // with a degree-bounded graph, we can measure things like assortativity. Each edge is joined using both of its endpoints.
            // this uses the "bounded-join", which imposes a limit on the number of records with each key, to bound the transformation's stability.
            var edgedegrees = securegraph.Join(nodes, edge => edge[0], node => node.Key, bound, bound, (edge, node) => new int[] { node.Count(), edge[1] })
                                         .Join(nodes, edge => edge[1], node => node.Key, bound, bound, (edge, node) => new int[] { edge[0], node.Count() });

            Console.WriteLine("Assortativity:");
            var srcparts = edgedegrees.Partition(Enumerable.Range(8,5).ToArray(), edge => edge[0]);
            foreach(var i in Enumerable.Range(8,5))
            {
                var dstparts = srcparts[i].Partition(Enumerable.Range(8,5).ToArray(), edge => edge[1]);
                foreach (var j in Enumerable.Range(8, 5))
                    Console.Write("\t{0:F2}", dstparts[j].NoisyCount(0.1));

                Console.WriteLine();
            }
            Console.WriteLine();


            // we can also measure the correlation coefficient: the number of triangles divided by the number of length two paths.
            var paths2 = ExtendPaths(bounded, bounded, bound, bound);
            var paths3 = ExtendPaths(paths2, bounded, bound * bound, bound);
            var triangles = paths3.Where(x => x[0] == x[3]);

            Console.WriteLine("Triangles:\t{0}", triangles.NoisyCount(0.1));
            Console.WriteLine("Len 2 paths:\t{0}", paths2.NoisyCount(0.1));
            Console.WriteLine();


            // one way to view pagerank is the sum over all paths arriving at a vertex, of the probability of 
            // traversing that path. usually this looks something like (alpha/degree)^length
            // although we'll have to have increasingly noisy counts with longer paths, to prevent privacy explosion, 
            // the contributions of these terms are scaled down commensurately.

            var depth = 3;
            var paths = new PINQueryable<int[]>[depth];
            paths[0] = bounded;
            foreach (var index in Enumerable.Range(1, depth - 1))
                paths[index] = ExtendPaths(paths[index - 1], bounded, Convert.ToInt32(Math.Pow(bound, index)), bound).Materialize();

            // for any set of endpoints (too small a set gives bad results, as privacy would dictate) we compute
            var pagerank = 0.0;
            foreach (var index in Enumerable.Range(0, depth))
            {
                pagerank += paths[index].Where(path => path.Last() % 10 == 0)
                                        .NoisyCount(0.1 * Math.Pow(0.85 / bound, index)) * Math.Pow(0.85 / bound, index);

                Console.WriteLine("pagerank using paths of length at most {0}:\t{1}", index + 1, pagerank);
            }

            Console.ReadKey();
        }
    }
}
