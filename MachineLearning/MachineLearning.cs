using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using PINQ;

namespace MachineLearning
{
    class Program
    {
        // generates an unbounded number of data points of specified dimension
        public static IEnumerable<double[]> GenerateData(int dimensions)
        {
            var RNG = new Random();
            while (true)
            {
                var vector = new double[dimensions];
                foreach (var index in Enumerable.Range(0, vector.Length))
                    vector[index] = 2.0 * RNG.NextDouble() - 1.0;

                yield return vector;
            }
        }

        public class Example
        {
            public double[] vector;
            public double label;

            public Example(double[] v, double l) { vector = v; label = l; }
        }

        // computes and steps along the gradient of 
        public static double[] PerceptronStep(PINQueryable<Example> input, double[] normal, double epsilon)
        {
            // select the examples that are currently mis-labeled by the normal vector
            var errors = input.Where(x => x.label * x.vector.Select((v, i) => v * normal[i]).Sum() < 0.0);

            // fold the average error into the normal
            var newnormal = new double[normal.Length];
            foreach (var coordinate in Enumerable.Range(0, normal.Length))
                newnormal[coordinate] = normal[coordinate] + errors.NoisyAverage(epsilon, x => x.label * x.vector[coordinate]);

            return newnormal;
        }


        // computes and steps along the gradient of the SVM objective function: Sum_i HingeLoss(1.0 - normal^Tx_i y_i) + ||w||_2^2
        public static double[] SupportVectorStep(PINQueryable<Example> input, double[] normal, double epsilon)
        {
            // select the examples that are currently mis-labeled by the normal vector. also add some negative normal for our regularizer
            var errors = input.Where(x => x.label * x.vector.Select((v, i) => v * normal[i]).Sum() < 1.0)
                              .Concat(Enumerable.Repeat(new Example(normal, -1.0), 10).AsQueryable());

            // fold the average error into the normal
            var newnormal = new double[normal.Length];
            foreach (var coordinate in Enumerable.Range(0, normal.Length))
                newnormal[coordinate] = normal[coordinate] + errors.NoisyAverage(epsilon, x => x.label * x.vector[coordinate]);

            return newnormal;
        }

        // computes and steps along the gradient of the logarithm of the Logistic Regression objective function
        public static double[] LogisticStep(PINQueryable<Example> input, double[] normal, double epsilon)
        {
            // compute the logistic probability of (xi, yi) under "normal", subtracted from (label + 1.0)/2.0 = target
            var errors = input.Select(x => new
            {
                vector = x.vector,
                error = (x.label + 1.0) / 2.0 - 1.0 / (1 + Math.Exp(-x.vector.Select((v, i) => v * normal[i]).Sum()))
            });

            // fold the average error into the normal
            var newnormal = new double[normal.Length];
            foreach (var coordinate in Enumerable.Range(0, normal.Length))
                newnormal[coordinate] = normal[coordinate] + errors.NoisySum(epsilon, x => x.error * x.vector[coordinate]);

            return newnormal;
        }

        // computes the inner product between vectors a and b
        public static double InnerProduct(double[] a, double[] b)
        {
            // written in LINQ to show it can be done.
            return a.Select((v, i) => v * b[i]).Sum();
        }

        // returns the nearest center to the input vector, using the L2 norm
        public static double[] NearestCenter(double[] vector, double[][] centers)
        {
            // written in LINQ to "prove it can be done". easier to see that it is functional, too
            return centers.Aggregate(centers[0], (old, cur) => (vector.Select((v, i) => v - old[i])
                                                                      .Select(diff => diff * diff)
                                                                      .Sum() >
                                                                vector.Select((v, i) => v - cur[i])
                                                                      .Select(diff => diff * diff)
                                                                      .Sum()) ? cur : old);
        }

        // runs one step of the iterative k-means algorithm. 
        public static void kMeansStep(PINQueryable<double[]> input, double[][] centers, double epsilon)
        {
            // partition data set by the supplied centers; somewhat icky in pure LINQ... (( and it assumes centers[0] exists ))
            var parts = input.Partition(centers, x => NearestCenter(x, centers));
            // update each of the centers
            foreach (var center in centers)
            {
                var part = parts[center];
                foreach (var index in Enumerable.Range(0, center.Length))
                    center[index] = part.NoisyAverage(epsilon, x => x[index]);
            }
        }

        // computes the average of the data along each of its coordinates.
        public static double[] Mean(PINQueryable<double[]> input, int dimensions, double epsilon)
        {
            double[] means = new double[dimensions];
            foreach (var i in Enumerable.Range(0, dimensions))
                means[i] = input.NoisyAverage(epsilon, x => x[i]);

            return means;
        }

        // computes the outer product of the data matrix with itself. if the data are centered, this is the covariance matrix
        public static double[][] Covariance(PINQueryable<double[]> input, int dimensions, double epsilon)
        {
            double[][] outer = new double[dimensions][];

            foreach (var i in Enumerable.Range(0, dimensions))
            {
                outer[i] = new double[dimensions];
                foreach (var j in Enumerable.Range(0, dimensions))
                    outer[i][j] = input.NoisyAverage(epsilon, x => x[i] * x[j]);
            }

            return outer;
        }

        static void Main(string[] args)
        {
            var dimensions = 8;
            var records = 10000;
            var sourcedata = GenerateData(dimensions).Take(records).ToArray().AsQueryable();
            var securedata = new PINQueryable<double[]>(sourcedata, null);

            // let's start by computing the centroid of the data
            var means = Mean(securedata, dimensions, 0.1);

            Console.WriteLine("mean vector:");
            foreach (var mean in means)
                Console.Write("\t{0:F4}", mean);
            Console.WriteLine();
            Console.WriteLine();


            // we can also center the data and compute its covariance
            var centered = securedata.Select(x => x.Select((v, i) => v - means[i]).ToArray());
            var covariance = Covariance(centered, dimensions, 8);

            Console.WriteLine("covariance matrix:");
            foreach (var row in covariance)
            {
                foreach (var entry in row)
                    Console.Write("\t{0:F4}", entry);
                Console.WriteLine();
            }
            Console.WriteLine();


            // iterative algorithms are also possible. we'll do k-means first
            var k = 3;
            var centers = GenerateData(dimensions).Take(k).ToArray();
            var iterations = 5;
            foreach (var iteration in Enumerable.Range(0, iterations))
                kMeansStep(securedata, centers, 0.1);

            Console.WriteLine("kMeans: {0} centers, {1} iterations", k, iterations);
            foreach (var center in centers)
            {
                foreach (var value in center)
                    Console.Write("\t{0:F4}", value);
                Console.WriteLine();
            }
            Console.WriteLine();


            // Moving to supervised learning, let's label the points by whether they are nearest the first center or not
            var labeled = securedata.Select(x => new Example(x, NearestCenter(x, centers) == centers[0] ? 1.0 : -1.0));

            // the Perceptron algorithm repeatedly adds misclassified examples to a normal vector
            var perceptronnormal = GenerateData(dimensions).First();
            foreach (var index in Enumerable.Range(0, iterations))
                perceptronnormal = PerceptronStep(labeled, perceptronnormal, 0.1);

            var perceptronerror = labeled.NoisyAverage(0.1, x => x.label * x.vector.Select((v, i) => v * perceptronnormal[i]).Sum() < 0.0 ? 1.0 : 0.0);
            Console.WriteLine("perceptron error rate:\t\t{0:F4}", perceptronerror);

            // the Support Vector Machine attempts to find a maximum margin classifier
            var supportvectornormal = GenerateData(dimensions).First();
            foreach (var index in Enumerable.Range(0, iterations))
                supportvectornormal = SupportVectorStep(labeled, supportvectornormal, 0.1);

            var supportvectorerror = labeled.NoisyAverage(0.1, x => x.label * x.vector.Select((v, i) => v * supportvectornormal[i]).Sum() < 0.0 ? 1.0 : 0.0);
            Console.WriteLine("support vector error rate:\t{0:F4}", supportvectorerror);

            // Logistic regression optimizes the likelihood of the labels under the logistic function
            var logisticnormal = GenerateData(dimensions).First();
            foreach (var index in Enumerable.Range(0, iterations))
                logisticnormal = LogisticStep(labeled, logisticnormal, 0.1);

            var logisticerror = labeled.NoisyAverage(0.1, x => x.label * x.vector.Select((v, i) => v * logisticnormal[i]).Sum() < 0.0 ? 1.0 : 0.0);
            Console.WriteLine("logistic error rate:\t\t{0:F4}", logisticerror);

            Console.ReadKey();
        }
    }
}
