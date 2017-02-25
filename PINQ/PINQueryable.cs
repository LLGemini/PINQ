using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Collections.ObjectModel;
namespace PINQ
{
    /// <summary>
    /// Used as a delegate for data sets, to respond to requests for private access to the data.
    /// Intended use is by subtyping and overriding the apply method to implement data set specific logic.
    /// </summary>
    public class PINQAgent
    {
        /// <summary>
        /// Method applies logic associated with a change of epsilon to the differential privacy bound of the associated data set.
        /// The intended semantics are that if the return value is true then the change is commited, if the return value is false it is not commited.
        /// Negative values may be passed in as a way of "rolling back" increments if they are later aborted. It is recommended that all negative changes be applied (the intent is that the only access to this method is through trusted code) as to fail to do so may result in excessively pessimistic privacy assessment.
        /// </summary>
        /// <param name="epsilon">Additive increment to differential privacy parameter. Both positive and negative values are possible. Can be both positive and negative.</param>
        /// <returns>True if the change is accepted, and false if not. It is assumed that negative changes will be accepted, and return values may not always be tested in that case.</returns>
        public virtual bool apply(double epsilon) { return true; }
    };

    #region Useful PINQAgents

    /// <summary>
    /// A PINQAgent maintaining a fixed budget. Any request that would exceed the budget is rejected. Accepted requests deplete the budget.
    /// </summary>
    public class PINQAgentBudget : PINQAgent
    {
        private double budget;

        /// <summary>
        /// Tests if the increment epsilon is acceptable. If so, the budget is decremented byfore returning.
        /// </summary>
        /// <param name="epsilon">epsilon</param>
        /// <returns>True iff the remain budget supports the decrement.</returns>
        public override bool apply(double epsilon)
        {
            if (epsilon > budget)
                return false;

            budget -= epsilon;
            return true;
        }
        /// <summary>
        /// Constructor for a PINQAgentBudget
        /// </summary>
        /// <param name="b">The initial budget setting.</param>
        public PINQAgentBudget(double b)
        {
            budget = b;
        }
    }

    /// <summary>
    /// A PINQAgent who scales all requests by a fixed value and passes them on.
    /// </summary>
    public class PINQAgentUnary : PINQAgent
    {
        private PINQAgent target;
        private double scale;

        /// <summary>
        /// Queries the target PINQAgent with a scaled value of epsilon.
        /// </summary>
        /// <param name="epsilon">epsilon</param>
        /// <returns>Accepts iff the target PINQAgent accepts the scaled request.</returns>
        public override bool apply(double epsilon)
        {
            return target.apply(epsilon * scale);
        }

        /// <summary>
        /// PINQAgentUnary Constructor
        /// </summary>
        /// <param name="t">Target PINQAgent, who handles scaled requests.</param>
        /// <param name="s">Scale factor to apply to requests.</param>
        public PINQAgentUnary(PINQAgent t, double s)
        {
            target = (t == null) ? new PINQAgent() : t;
            scale = s;
        }
    };

    /// <summary>
    /// A PINQAgent with two targets, both of who must agree to any increment.
    /// </summary>
    public class PINQAgentBinary : PINQAgent
    {
        private PINQAgent targetA;
        private PINQAgent targetB;

        /// <summary>
        /// Tests both targets, accepting if both accept.
        /// </summary>
        /// <param name="epsilon">epsilon</param>
        /// <returns>Accepts iff both target PINQAgents accept.</returns>
        public override bool apply(double epsilon)
        {
            if (targetA.apply(epsilon))
            {
                if (targetB.apply(epsilon))
                    return true;
                else
                    targetA.apply(-epsilon);    // important to reverse targetA's commitment
            }
            return false;
        }

        /// <summary>
        /// PINQAgentBinary constructor
        /// </summary>
        /// <param name="a">First target</param>
        /// <param name="b">Second target</param>
        public PINQAgentBinary(PINQAgent a, PINQAgent b)
        {
            targetA = (a == null) ? new PINQAgent() : a;
            targetB = (b == null) ? new PINQAgent() : b;
        }
    };

    /// <summary>
    /// PINQAgent class resulting from the Partition operation. 
    /// Contains a list of epsilon values, and tracks the maximum value.
    /// Increments to the maximum are forwarded to the source IQueryable.
    /// Requests that do not increment the maximum are accepted.
    /// </summary>
    /// <typeparam name="K">The type of the key used to partition the data set.</typeparam>
    public class PINQAgentPartition<K> : PINQAgent
    {
        private PINQAgent target;               // agent of data source that has been partitioned.

        private double[] maximum;               // should be shared
        private Dictionary<K, double> table;    // dictionary shared among several PINQAgentPartitions.
        private K key;                          // key associated with *this* PINQAgentPartition.

        /// <summary>
        /// Accepts iff the increment to the maximum value is accepted by the target.
        /// </summary>
        /// <param name="epsilon">epsilon</param>
        /// <returns>Accepts if the increment to the maximum value is accepted by the target.</returns>
        public override bool apply(double epsilon)
        {
            // if we increment the maximum, test and update
            if (table[key] + epsilon > maximum[0])
            {
                if (target.apply((table[key] + epsilon) - maximum[0]))
                {
                    table[key] += epsilon;
                    maximum[0] = table[key];
                    return true;
                }

                return false;
            }

            // if we were the maximum, and we decrement, re-establish the maximum.
            if (table[key] == maximum[0] && epsilon < 0.0)
            {
                table[key] += epsilon;
                maximum[0] = table.Select(x => x.Value).Max();
            }
            else
                table[key] += epsilon;

            return true;
        }

        /// <summary>
        /// Constructor for PINQAgentPartition
        /// </summary>
        /// <param name="t">Target PINQAgent</param>
        /// <param name="tbl">Table of (key,epsilon) pairs</param>
        /// <param name="k">Key associated with this agent</param>
        /// <param name="m">Stores a shared maximum between all peers</param>
        public PINQAgentPartition(PINQAgent t, Dictionary<K, double> tbl, K k, double[] m)
        {
            target = (t == null) ? new PINQAgent() : t;
            table = tbl;
            key = k;
            maximum = m;
        }
    };

    #endregion

    /// <summary>
    /// Wrapper for IQueryable generic that helps to maintain differential privacy.
    /// Interface intends to parallel the LINQ standard query operators to a large degree, with additional methods specific to differential privacy.
    /// </summary>
    /// <typeparam name="T">The generic type of the records of the associated IQueryable.</typeparam>
    public class PINQueryable<T>
    {
        /// <summary>
        /// Data source, to which we apply operations.
        /// </summary>
        protected IQueryable<T> source;
        /// <summary>
        /// Privacy agent, who must confirm all accesses.
        /// </summary>
        protected PINQAgent agent;
        /// <summary>
        /// Rewriting method, applied to all user-supplied expressions before execution.
        /// Intended to be used to mitigate exploits and side-channel attacks.
        /// </summary>
        protected Func<Expression, Expression> rewrite;

        /// <summary>
        /// Random number generator for all randomized query responses. Consider strengthening as appopriate.
        /// </summary>
        protected static System.Random random = new System.Random();

        #region Helpful Methods
        // private helper methods that add noise, and merge lists, respectively.
        private double Laplace(double stddev)
        {
            double uniform = random.NextDouble() - 0.5;
            return stddev * Math.Sign(uniform) * Math.Log(1 - 2.0 * Math.Abs(uniform));
        }
        private double Uniform(double low, double high)
        {
            return low + (high - low) * random.NextDouble();
        }
        #endregion

        #region Aggregations

        /* These methods are the only way to extract information reflecting the contents of the data sets. */

        /// <summary>
        /// Counts the number of tuples in the source data set, with noise added for privacy.
        /// </summary>
        /// <param name="epsilon">The accuracy of the associated noise, influencing the privacy lost.</param>
        /// <returns>The count of the source data set, plus Laplace(1.0/epsilon).</returns>
        public double NoisyCount(double epsilon)
        {
            if (epsilon < 0.0)
                throw new Exception("Negative epsilon not permitted");

            // Commence the agent process.
            if (agent.apply(epsilon))
                return source.Count() + Laplace(1.0 / epsilon);   // actually add some noise here eventually
            else
                throw new Exception("PINQ access denied");
        }

        /// <summary>
        /// Computes a noisy average resulting from the application of function to each record.
        /// The function is tested against the expression visitor, and after application each output value is clamped to [-1,+1].
        /// </summary>
        /// <param name="epsilon">Determines the accuracy of the average, and the privacy lost</param>
        /// <param name="function">Function to apply to each tuple, yield a number in [0,1]</param>
        /// <returns>The average of the [0,1] values described by function, plus Laplace(2.0/epsilon)/Count.</returns>
        public double NoisyAverage(double epsilon, Expression<Func<T, double>> function)
        {
            if (epsilon < 0.0)
                throw new Exception("Negative epsilon not permitted");

            function = rewrite(function) as Expression<Func<T, double>>;

            if (agent.apply(epsilon))
            {
                // apply the function, and then clamp values to the [-1,+1] interval.
                IQueryable<double> values = source.Select(function)
                                                  .Select(x => x > +1.0 ? +1.0 : x)
                                                  .Select(x => x < -1.0 ? -1.0 : x);

                double tally = values.Sum();    // we'll want this, maybe multiple times.
                double count = values.Count();  // we'll want this, maybe multiple times.

                // if the count is zero, we need to select uniformly at random from [-1,+1].
                if (count == 0)
                    return Uniform(-1.0, +1.0);

                double candidate = (tally + Laplace(2.0 / epsilon)) / count;
                while (candidate < -1.0 || candidate > 1.0)
                    candidate = (tally + Laplace(2.0 / epsilon)) / count;

                return candidate;
            }
            else
                throw new Exception("PINQ access denied");
        }

        /// <summary>
        /// Computes a noisy sum resulting from the application of function to each record.
        /// The function is first tested again the expression visitor, and the output of each invocation is clamped to [-1,+1].
        /// </summary>
        /// <param name="epsilon">Determines the accuracy of the sum, and privacy lost.</param>
        /// <param name="function">Function to be applied to each record. Results are clamped to [-1.0,+1.0].</param>
        /// <returns>The clamped sums of the application of function to each record, plus Laplace(1.0/epsilon).</returns>
        public double NoisySum(double epsilon, Expression<Func<T, double>> function)
        {
            if (epsilon < 0.0)
                throw new Exception("Negative epsilon not permitted");

            function = rewrite(function) as Expression<Func<T, double>>;

            if (agent.apply(epsilon))
            {
                IQueryable<double> values = source.Select(function)
                                                  .Select(x => x > +1.0 ? +1.0 : x)
                                                  .Select(x => x < -1.0 ? -1.0 : x);

                return values.Sum() + Laplace(1.0 / epsilon);
            }
            else
                throw new Exception("PINQ access denied");
        }

        /// <summary>
        /// Computes a value in [0,1] that splits the input at roughly the intended fraction.
        /// </summary>
        /// <param name="epsilon">Amount of privacy lost. Controls the sensitivity of the computation.</param>
        /// <param name="fraction">Value in [0,1] indicating the fraction of data to be split. 0.5 would be median.</param>
        /// <param name="function">Function to produce values in [0,1] from the source tuples.</param>
        /// <returns>Randomly selected element of [0,1] with exponential bias towards values that split with the appropriate balance.</returns>
        public double NoisyOrderStatistic(double epsilon, double fraction, Expression<Func<T, double>> function)
        {
            if (epsilon < 0.0)
                throw new Exception("Negative epsilon not permitted");

            function = rewrite(function) as Expression<Func<T, double>>;

            if (agent.apply(epsilon))
            {
                double target = source.Count() * fraction;
                IQueryable<double> values = source.Select(function).
                                                   Select(x => (x < 0.0) ? 0.0 : x).
                                                   Select(x => (x > 1.0) ? 1.0 : x);

                IQueryable<double> sorted = values.OrderBy(x => x);

                // we now write a custom aggregator that reservoir samples from the sorted list
                var result = sorted.Aggregate(new { previous = 0.0, counter = 0.0, sample = 0.0, tally = 0.0 },
                                              (state, value) => new
                                              {
                                                  previous = value,
                                                  counter = state.counter + 1.0,
                                                  sample = (random.NextDouble() > state.tally / (state.tally + (value - state.previous) * Math.Exp(-epsilon * Math.Abs(target - state.counter)))) ? (value - state.previous) * random.NextDouble() + state.previous : state.sample,
                                                  tally = state.tally + (value - state.previous) * Math.Exp(-epsilon * Math.Abs(target - state.counter))
                                              });

                if (random.NextDouble() > result.tally / (result.tally + (1.0 - result.previous) * Math.Exp(-epsilon * Math.Abs(target - result.counter))))
                    return (1.0 - result.previous) * random.NextDouble() + result.previous;
                else
                    return (result.sample);
            }
            else
                throw new Exception("PINQ access denied");
        }

        /// <summary>
        /// Computes a value in [0,1] that splits the data approximately in half. Uses NoisyOrderStatistic.
        /// </summary>
        /// <param name="epsilon">Amount of privacy lost. Controls the sensitivity of the computation.</param>
        /// <param name="function">Function to produce values in [0,1] from the source tuples.</param>
        /// <returns>Randomly selected element of [0,1] with exponential bias towards those that spilt the data into equal sized parts.</returns>
        public double NoisyMedian(double epsilon, Expression<Func<T, double>> function)
        {
            if (epsilon < 0.0)
                throw new Exception("Negative epsilon not permitted");

            return NoisyOrderStatistic(epsilon, 0.5, function);
        }

        /// <summary>
        /// Selects an element of range using the provided score function to evaluate each option against the tuples in the data set.
        /// Output is selected with probability proportional to the exponential of epsilon times the summed score function applied to each tuple in the data set.
        /// </summary>
        /// <typeparam name="R">Type of the output.</typeparam>
        /// <param name="epsilon">Describes the accuracy of the mechanism, by dampening the influence of the score function, as well as the privacy loss.</param>
        /// <param name="range">Set of possible outputs.</param>
        /// <param name="scoreFunc">Function that scores each of the tuples of the data set against each of the possible results.</param>
        /// <returns>Returns a random element of range, with probability exponentially favoring those elements that score well in aggregate with tuples in the data set.</returns>
        public R ExponentialMechanism<R>(double epsilon, IQueryable<R> range, Expression<Func<T, R, double>> scoreFunc)
        {
            if (epsilon < 0.0)
                throw new Exception("Negative epsilon not permitted");

            scoreFunc = rewrite(scoreFunc) as Expression<Func<T, R, double>>;

            if (agent.apply(epsilon))
            {
                // this compilation is "required" for performance. However, it appears to crash Mono.
                var compiled = scoreFunc.Compile();

                var scores = range.Select(r => new KeyValuePair<R, double>(r, source.Select(x => compiled.Invoke(x, r))
                                                                                    .Select(x => x < 0.0 ? 0.0 : x)
                                                                                    .Select(x => x > 1.0 ? 1.0 : x)
                                                                                    .Sum())).ToArray();

                var noised = scores.Select(kvp => new KeyValuePair<R, double>(kvp.Key, kvp.Value + Laplace(1.0 / epsilon)))
                                   .ToArray();

                return noised.Aggregate((x, y) => x.Value > y.Value ? x : y).Key;
            }
            else
                throw new Exception("PINQ access denied");
        }

        #endregion

        #region Transformations

        /* These methods manipulate the query, but do not actually return data. 
         * They may or may not actually invoke data processing. Can be defered. */

        #region Restriction Transformations

        /// <summary>
        /// LINQ Where transformation.
        /// </summary>
        /// <param name="predicate">boolean predicate applied to each record</param>
        /// <returns>PINQueryable containing the subset of records satisfying the input predicate.</returns>
        public PINQueryable<T> Where(Expression<Func<T, bool>> predicate)
        {
            predicate = rewrite(predicate) as Expression<Func<T, bool>>;

            return NewPINQueryable<T>(source.Where(predicate), new PINQAgentUnary(agent, 1.0));
        }
        #endregion

        #region Projection Transformations

        /// <summary>
        /// LINQ Select transformation.
        /// </summary>
        /// <typeparam name="S">Result type of the underlying records.</typeparam>
        /// <param name="selector">Record-to-record transformation</param>
        /// <returns>PINQueryable containing the transformations of records in the source data set.</returns>
        public PINQueryable<S> Select<S>(Expression<Func<T, S>> selector)
        {
            selector = rewrite(selector) as Expression<Func<T, S>>;

            return NewPINQueryable<S>(source.Select(selector), new PINQAgentUnary(agent, 1.0));
        }

        /// <summary>
        /// LINQ SelectMany transformation. Each record may only produce a limited number of records.
        /// </summary>
        /// <typeparam name="S">Result type of the underlying records.</typeparam>
        /// <param name="k">Upper bound on number of records produced by each input record.</param>
        /// <param name="selector">Record-to-RecordList transformation</param>
        /// <returns>PINQueryable containing the first k records produced from each source record.</returns>
        public PINQueryable<S> SelectMany<S>(int k, Expression<Func<T, IEnumerable<S>>> selector)
        {
            selector = rewrite(selector) as Expression<Func<T, IEnumerable<S>>>;

            // produce at most k elements for each input record
            IQueryable<S> selected = source.Select(selector)
                                           .Select(x => x.Take(k))
                                           .SelectMany(x => x);

            return NewPINQueryable<S>(selected, new PINQAgentUnary(agent, k));
        }
        #endregion

        #region Partitioning Transformations

        /// <summary>
        /// LINQ Take transformation.
        /// </summary>
        /// <param name="count">Number of records to return</param>
        /// <returns>PINQueryable containing the first count records.</returns>
        public PINQueryable<T> Take(int count)
        {
            return NewPINQueryable<T>(source.Take(count), agent);
        }

        /// <summary>
        /// LINQ Skip transformation
        /// </summary>
        /// <param name="count">Number of records to skip</param>
        /// <returns>PINQueryable containing all but the first count records.</returns>
        public PINQueryable<T> Skip(int count)
        {
            return NewPINQueryable<T>(source.Skip(count), agent);
        }

        #endregion

        #region Join Transformations

        /// <summary>
        /// LINQ Join with an unprotected IQueryable.
        /// </summary>
        /// <typeparam name="S">Other record type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Second data set</param>
        /// <param name="keySelector1">First key selector</param>
        /// <param name="keySelector2">Second key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>PINQueryable containing the Join of the two data sets, each first GroupBy'd using their key selector functions.</returns>
        public PINQueryable<R> Join<S, K, R>(IQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, T>, IGrouping<K, S>, R>> resultSelector)
        {
            keySelector1 = rewrite(keySelector1) as Expression<Func<T, K>>;
            keySelector2 = rewrite(keySelector2) as Expression<Func<S, K>>;
            resultSelector = rewrite(resultSelector) as Expression<Func<IGrouping<K, T>, IGrouping<K, S>, R>>;

            IQueryable<IGrouping<K, T>> s1Grouped = source.GroupBy(keySelector1);
            IQueryable<IGrouping<K, S>> s2Grouped = other.GroupBy(keySelector2);

            IQueryable<R> result = s1Grouped.Join(s2Grouped, x => x.Key, x => x.Key, resultSelector);

            return NewPINQueryable<R>(result, new PINQAgentUnary(agent, 2.0));
        }

        /// <summary>
        /// LINQ Join with an unprotected IQueryable, but an intended PINQAgent protecting it. 
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Second data set</param>
        /// <param name="otherAgent">Second data set's PINQAgent</param>
        /// <param name="keySelector1">First key selector</param>
        /// <param name="keySelector2">Second key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns></returns>
        public PINQueryable<R> Join<S, K, R>(IQueryable<S> other, PINQAgent otherAgent, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, T>, IGrouping<K, S>, R>> resultSelector)
        {
            keySelector1 = rewrite(keySelector1) as Expression<Func<T, K>>;
            keySelector2 = rewrite(keySelector2) as Expression<Func<S, K>>;
            resultSelector = rewrite(resultSelector) as Expression<Func<IGrouping<K, T>, IGrouping<K, S>, R>>;

            IQueryable<IGrouping<K, T>> s1Grouped = source.GroupBy<T, K>(keySelector1);
            IQueryable<IGrouping<K, S>> s2Grouped = other.GroupBy<S, K>(keySelector2);

            IQueryable<R> result = s1Grouped.Join(s2Grouped, x => x.Key, x => x.Key, resultSelector);

            return NewPINQueryable<R>(result, new PINQAgentBinary(new PINQAgentUnary(agent, 2.0), new PINQAgentUnary(otherAgent, 2.0)));
        }

        /// <summary>
        /// Entrypoint for Join with a PINQueryable. Passes control to the other PINQueryable to expose its IQueryable.
        /// </summary>
        /// <typeparam name="S">Second data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Second PINQueryable</param>
        /// <param name="keySelector1">First key selector</param>
        /// <param name="keySelector2">Second key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>Passes control to other PINQueryable. Intends to return a PINQueryable containing the Join of the two data sets, after each is GroupBy'd using their key selectors.</returns>
        public PINQueryable<R> Join<S, K, R>(PINQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, T>, IGrouping<K, S>, R>> resultSelector)
        {
            return other.JoinHelper(this, keySelector2, keySelector1, resultSelector);
        }

        /// <summary>
        /// Helper method for Join. Invokes first PINQueryable's join on its unprotected data.
        /// </summary>
        /// <typeparam name="S">First data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">First data set</param>
        /// <param name="keySelector1">This key selector</param>
        /// <param name="keySelector2">Other key selectar</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>Invokes the other PINQueryable's Join method with unprotected data, and returns its result.</returns>
        public virtual PINQueryable<R> JoinHelper<S, K, R>(PINQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, S>, IGrouping<K, T>, R>> resultSelector)
        {
            // place a smart test for the concrete types that you trust to execute your join.
            return other.Join(source, agent, keySelector2, keySelector1, resultSelector);
        }

        /// <summary>
        /// Bounded Join with unprotected IQueryable and PINQAgent intended to protect it. The join imposes a limit on the number of records from each data set mapping to each key. 
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="otherAgent">Other data set's PINQAgent</param>
        /// <param name="keySelector1">First key selector</param>
        /// <param name="keySelector2">Secord key selector</param>
        /// <param name="bound1">bound on per-key records in first data set</param>
        /// <param name="bound2">bound on pre-key records in second data set</param>
        /// <param name="resultSelector">result selector</param>
        /// <returns>PINQueryable containing the LINQ JOIN of the first bound1 and bound2 records for each key, from the two data sets.</returns>
        public PINQueryable<R> Join<S, K, R>(IQueryable<S> other, PINQAgent otherAgent, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, int bound1, int bound2, Expression<Func<T, S, R>> resultSelector)
        {
            keySelector1 = rewrite(keySelector1) as Expression<Func<T, K>>;
            keySelector2 = rewrite(keySelector2) as Expression<Func<S, K>>;
            resultSelector = rewrite(resultSelector) as Expression<Func<T, S, R>>;

            IQueryable<T> source1 = source.GroupBy<T, K>(keySelector1).SelectMany(group => group.Take(bound1));
            IQueryable<S> source2 = other.GroupBy<S, K>(keySelector2).SelectMany(group => group.Take(bound2));

            IQueryable<R> result = source1.Join(source2, keySelector1, keySelector2, resultSelector);

            return NewPINQueryable<R>(result, new PINQAgentBinary(new PINQAgentUnary(agent, 2.0 * bound2), new PINQAgentUnary(otherAgent, 2.0 * bound1)));
        }

        /// <summary>
        /// Bounded Join with unprotected IQueryable and PINQAgent intended to protect it. The join imposes a limit on the number of records from each data set mapping to each key. 
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="keySelector1">First key selector</param>
        /// <param name="keySelector2">Secord key selector</param>
        /// <param name="bound1">bound on per-key records in first data set</param>
        /// <param name="bound2">bound on pre-key records in second data set</param>
        /// <param name="resultSelector">result selector</param>
        /// <returns>PINQueryable containing the LINQ JOIN of the first bound1 and bound2 records for each key, from the two data sets.</returns>
        public PINQueryable<R> Join<S, K, R>(IQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, int bound1, int bound2, Expression<Func<T, S, R>> resultSelector)
        {
            keySelector1 = rewrite(keySelector1) as Expression<Func<T, K>>;
            keySelector2 = rewrite(keySelector2) as Expression<Func<S, K>>;
            resultSelector = rewrite(resultSelector) as Expression<Func<T, S, R>>;

            IQueryable<T> source1 = source.GroupBy<T, K>(keySelector1).SelectMany(group => group.Take(bound1));
            IQueryable<S> source2 = other.GroupBy<S, K>(keySelector2).SelectMany(group => group.Take(bound2));

            IQueryable<R> result = source1.Join(source2, keySelector1, keySelector2, resultSelector);

            return NewPINQueryable<R>(result, new PINQAgentUnary(agent, 2.0 * bound2));
        }

        /// <summary>
        /// Bounded Join with unprotected IQueryable and PINQAgent intended to protect it. The join imposes a limit on the number of records from each data set mapping to each key. 
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="keySelector1">First key selector</param>
        /// <param name="keySelector2">Secord key selector</param>
        /// <param name="bound1">bound on per-key records in first data set</param>
        /// <param name="bound2">bound on pre-key records in second data set</param>
        /// <param name="resultSelector">result selector</param>
        /// <returns>PINQueryable containing the LINQ JOIN of the first bound1 and bound2 records for each key, from the two data sets.</returns>
        public PINQueryable<R> Join<S, K, R>(PINQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, int bound1, int bound2, Expression<Func<T, S, R>> resultSelector)
        {
            return other.JoinHelper(this, keySelector2, keySelector1, bound2, bound1, resultSelector);
        }
        /// <summary>
        /// Bounded Join with unprotected IQueryable and PINQAgent intended to protect it. The join imposes a limit on the number of records from each data set mapping to each key. 
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="keySelector1">First key selector</param>
        /// <param name="keySelector2">Secord key selector</param>
        /// <param name="bound1">bound on per-key records in first data set</param>
        /// <param name="bound2">bound on pre-key records in second data set</param>
        /// <param name="resultSelector">result selector</param>
        /// <returns>PINQueryable containing the LINQ JOIN of the first bound1 and bound2 records for each key, from the two data sets.</returns>
        public PINQueryable<R> JoinHelper<S, K, R>(PINQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, int bound1, int bound2, Expression<Func<S, T, R>> resultSelector)
        {
            return other.Join(source, agent, keySelector2, keySelector1, bound2, bound1, resultSelector);
        }


        /// <summary>
        /// LINQ GroupJoin with an unprotected IQueryable.
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other IQueryable</param>
        /// <param name="keySelector1">This key selector</param>
        /// <param name="keySelector2">Other key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>PINQueryable containing the GroupJoin with the unprotected IQueryable.</returns>
        public PINQueryable<R> GroupJoin<S, K, R>(IQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<T, IEnumerable<S>, R>> resultSelector)
        {
            keySelector1 = rewrite(keySelector1) as Expression<Func<T, K>>;
            keySelector2 = rewrite(keySelector2) as Expression<Func<S, K>>;
            resultSelector = rewrite(resultSelector) as Expression<Func<T, IEnumerable<S>, R>>;

            IQueryable<R> result = source.GroupJoin(other, keySelector1, keySelector2, resultSelector);

            return NewPINQueryable<R>(result, new PINQAgentUnary(agent, 1.0));
        }

        /// <summary>
        /// LINQ GroupJoin with an unprotected IQueryable, and result selector that expects pairs of groups.
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="keySelector1">This key selector</param>
        /// <param name="keySelector2">Other key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>PINQueryable containing the GroupJoin with the unprotected IQueryable, the first having been GroupBy'd its key selector.</returns>
        public PINQueryable<R> GroupJoin<S, K, R>(IQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, T>, IEnumerable<S>, R>> resultSelector)
        {
            keySelector1 = rewrite(keySelector1) as Expression<Func<T, K>>;
            keySelector2 = rewrite(keySelector2) as Expression<Func<S, K>>;
            resultSelector = rewrite(resultSelector) as Expression<Func<IGrouping<K, T>, IEnumerable<S>, R>>;

            IQueryable<R> result = source.GroupBy(keySelector1)
                                         .GroupJoin(other, x => x.Key, keySelector2, resultSelector);

            return NewPINQueryable<R>(result, new PINQAgentUnary(agent, 2.0));
        }

        /// <summary>
        /// LINQ GroupJoin with an unprotected IQueryable and a PINQAgent intended to protect that IQueryable.
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="otherAgent">Other data set's PINQAgent</param>
        /// <param name="keySelector1">This key selector</param>
        /// <param name="keySelector2">Other key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>PINQueryable containing the GroupJoin of the two data sets, the first having been GroupBy'd its key selector.</returns>
        public PINQueryable<R> GroupJoin<S, K, R>(IQueryable<S> other, PINQAgent otherAgent, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, T>, IEnumerable<S>, R>> resultSelector)
        {
            keySelector1 = rewrite(keySelector1) as Expression<Func<T, K>>;
            keySelector2 = rewrite(keySelector2) as Expression<Func<S, K>>;
            resultSelector = rewrite(resultSelector) as Expression<Func<IGrouping<K, T>, IEnumerable<S>, R>>;

            IQueryable<R> result = source.GroupBy(keySelector1)
                                         .GroupJoin(other, x => x.Key, keySelector2, resultSelector);

            return NewPINQueryable<R>(result, new PINQAgentBinary(new PINQAgentUnary(agent, 2.0), new PINQAgentUnary(otherAgent, 2.0)));
        }

        /// <summary>
        /// LINQ GroupJoin entry point.
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="keySelector1">This key selector</param>
        /// <param name="keySelector2">Other key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>Passes control to other PINQueryable. Intends to result in the GroupJoin of the two, each GroupBy'd their key selectors.</returns>
        public PINQueryable<R> GroupJoin<S, K, R>(PINQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, T>, IEnumerable<S>, R>> resultSelector)
        {
            return other.GroupJoinHelper(this, keySelector2, keySelector1, resultSelector);
        }

        /// <summary>
        /// GroupJoin helper method. Passes control to the other PINQueryable, with an unprotected data set.
        /// </summary>
        /// <typeparam name="S">Other data type</typeparam>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="R">Result type</typeparam>
        /// <param name="other">Other data set</param>
        /// <param name="keySelector1">This key selector</param>
        /// <param name="keySelector2">Other key selector</param>
        /// <param name="resultSelector">Result selector</param>
        /// <returns>Passes control to the other PINQueryable. Intends to return the GroupJoin between the two, each GroupBy'd their key selectors.</returns>
        public virtual PINQueryable<R> GroupJoinHelper<S, K, R>(PINQueryable<S> other, Expression<Func<T, K>> keySelector1, Expression<Func<S, K>> keySelector2, Expression<Func<IGrouping<K, S>, IEnumerable<T>, R>> resultSelector)
        {
            return other.GroupJoin(source, keySelector2, keySelector1, resultSelector);
        }
        #endregion

        #region Concatenation Transformations

        /// <summary>
        /// LINQ Concat with an unprotected IQueryable.
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <returns>PINQueryable containing the concatenation of the two data sets.</returns>
        public PINQueryable<T> Concat(IQueryable<T> other)
        {
            return NewPINQueryable<T>(source.Concat(other), agent);
        }

        /// <summary>
        /// LINQ Concat with an unprotected IQueryable, and a PINQAgent meant to protect it.
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <param name="otherAgent">Other IQueryable's PINQAgent</param>
        /// <returns>PINQueryable containing the concatenation of the two data sets.</returns>
        public PINQueryable<T> Concat(IQueryable<T> other, PINQAgent otherAgent)
        {
            return NewPINQueryable<T>(source.Concat(other), new PINQAgentBinary(agent, otherAgent));
        }

        /// <summary>
        /// LINQ Concat entry point.
        /// </summary>
        /// <param name="other">Other PINQueryable</param>
        /// <returns>Passes control to other PINQueryable. Intends to return a PINQueryable containing their concatenation.</returns>
        public PINQueryable<T> Concat(PINQueryable<T> other)
        {
            return other.ConcatHelper(this);
        }

        /// <summary>
        /// Concat helper method. Passes control to the other PINQueryable with an unprotected IQueryable and PINQAgent.
        /// </summary>
        /// <param name="first">Other PINQueryable</param>
        /// <returns>Passes control to other PINQueryable. Intends to return a PINQueryable containing their concatenation.</returns>
        public virtual PINQueryable<T> ConcatHelper(PINQueryable<T> first)
        {
            return first.Concat(source, agent);
        }
        #endregion

        #region Ordering Transformations
        /* [disabled due to uncertainty about comparison operators and stability of sorting] */
        /*
        public PINQueryable<T> OrderBy<K>(Expression<Func<T, K>> keySelector)
        {
            keySelector = rewrite(keySelector) as Expression<Func<T, K>>;

            return NewPINQueryable<T>(source.OrderBy<T, K>(keySelector), agent);
        }
        public PINQueryable<T> OrderByDescending<K>(Expression<Func<T, K>> keySelector)
        {
            keySelector = rewrite(keySelector) as Expression<Func<T, K>>;

            return NewPINQueryable<T>(source.OrderByDescending<T, K>(keySelector), agent);
        }
        public PINQueryable<T> ThenBy<K>(Expression<Func<T, K>> keySelector)
        {
            keySelector = rewrite(keySelector) as Expression<Func<T, K>>;

            // some concern that the input type might be data dependent (IOrderedQueryable)
            return NewPINQueryable<T>(ordered.ThenBy<T, K>(keySelector), agent);
        }
        public PINQueryable<T> ThenByDescending<K>(Expression<Func<T, K>> keySelector)
        {
            keySelector = rewrite(keySelector) as Expression<Func<T, K>>;

            return NewPINQueryable<T>(ordered.ThenByDescending<T, K>(keySelector), agent);
        }
        */
        #endregion

        #region Grouping Transformations

        /// <summary>
        /// LINQ GroupBy operation.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <param name="keySelector">Key selector</param>
        /// <returns>PINQueryable containing a list of groups, one for each observed key, of the records mapping to that key.</returns>
        public PINQueryable<IGrouping<K, T>> GroupBy<K>(Expression<Func<T, K>> keySelector)
        {
            keySelector = rewrite(keySelector) as Expression<Func<T, K>>;

            return NewPINQueryable<IGrouping<K, T>>(source.GroupBy(keySelector), new PINQAgentUnary(agent, 2.0));
        }

        #endregion

        #region Set Transformations

        /// <summary>
        /// LINQ Distinct
        /// </summary>
        /// <returns>PINQueryable containing the distinct set of elements</returns>
        public PINQueryable<T> Distinct()
        {
            return NewPINQueryable<T>(source.Distinct(), agent);
        }

        /// <summary>
        /// LINQ Distinct up to k elements
        /// </summary>
        /// <param name="k">max number of elements</param>
        /// <returns>Set of at most k of each elements</returns>
        public PINQueryable<T> Distinct(int k)
        {
            return NewPINQueryable<T>(source.GroupBy(x => x).SelectMany(group => group.Take(k)), agent);
        }

        /// <summary>
        /// Distinct with key selector
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <param name="k">max number of elements</param>
        /// <param name="keySelector">key selector to distinct by</param>
        /// <returns>Distinct with key function used rather than the elements</returns>
        public PINQueryable<T> Distinct<K>(int k, Expression<Func<T, K>> keySelector)
        {
            return NewPINQueryable<T>(source.GroupBy(keySelector).SelectMany(group => group.Take(k)), new PINQAgentUnary(agent, 2.0));
        }

        /// <summary>
        /// LINQ Union with an unprotected IQueryable.
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <returns>PINQueryable containing the union of the two data sets.</returns>
        public PINQueryable<T> Union(IQueryable<T> other)
        {
            return NewPINQueryable<T>(source.Union(other), agent);
        }

        /// <summary>
        /// LINQ Union with an unprotected IQueryable, and a PINQAgent intended to protect it.
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <param name="otherAgent">Other IQueryable's PINQAgent</param>
        /// <returns>PINQueryable containing the union of the two data sets.</returns>
        public PINQueryable<T> Union(IQueryable<T> other, PINQAgent otherAgent)
        {
            return NewPINQueryable<T>(source.Union(other), new PINQAgentBinary(agent, otherAgent));
        }

        /// <summary>
        /// LINQ Union entry point
        /// </summary>
        /// <param name="other">Other PINQueryable</param>
        /// <returns>Passes control to other PINQueryable. Intends to return a PINQueryable containing their union.</returns>
        public PINQueryable<T> Union(PINQueryable<T> other)
        {
            return other.UnionHelper(this);
        }

        /// <summary>
        /// Union Helper. Passes control to other PINQueryable with an unprotected IQueryable.
        /// </summary>
        /// <param name="first">Other PINQueryable</param>
        /// <returns>Passes control to other PINQueryable. Intends to return a PINQueryable containing their union.</returns>
        public virtual PINQueryable<T> UnionHelper(PINQueryable<T> first)
        {
            return first.Union(source, agent);
        }

        /// <summary>
        /// LINQ Intersect with an unprotected IQueryable
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <returns>PINQueryable containing the intersection of the two data sets.</returns>
        public PINQueryable<T> Intersect(IQueryable<T> other)
        {
            return NewPINQueryable<T>(source.Intersect(other), agent);
        }

        /// <summary>
        /// LINQ Intersect with an unprotected IQueryable and a PINQAgent intended to protect it.
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <param name="otherAgent">Other IQueryable's PINQAgent</param>
        /// <returns>PINQueryable containing the intersection of the two data sets.</returns>
        public PINQueryable<T> Intersect(IQueryable<T> other, PINQAgent otherAgent)
        {
            return NewPINQueryable<T>(source.Intersect(other), new PINQAgentBinary(agent, otherAgent));
        }

        /// <summary>
        /// LINQ Intersect entry point.
        /// </summary>
        /// <param name="other">Otehr PINQueryable</param>
        /// <returns>Passes control to the other PINQueryable. Intends to return a PINQueryable containing their intersection.</returns>
        public PINQueryable<T> Intersect(PINQueryable<T> other)
        {
            return other.IntersectHelper(this);
        }

        /// <summary>
        /// Intersect helper function. Passes control to the other PINQueryable with an unprotected IQueryable.
        /// </summary>
        /// <param name="first">Other PINQueryable</param>
        /// <returns>Passes control to the other PINQueryable. Intends to return a PINQueryable containing their intersection.</returns>
        public virtual PINQueryable<T> IntersectHelper(PINQueryable<T> first)
        {
            return first.Intersect(source, agent);
        }

        /// <summary>
        /// LINQ Except with an unprotected IQueryable
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <returns>PINQueryable containing all records except those in other.</returns>
        public PINQueryable<T> Except(IQueryable<T> other)
        {
            return NewPINQueryable<T>(source.Except(other), agent);
        }

        /// <summary>
        /// LINQ Except with an unprotected IQueryable and a PINQAgent intended to protect it.
        /// </summary>
        /// <param name="other">Other IQueryable</param>
        /// <param name="otherAgent">Other IQueryable's PINQAgent</param>
        /// <returns>PINQueryable containing all records except those in other.</returns>
        public PINQueryable<T> Except(IQueryable<T> other, PINQAgent otherAgent)
        {
            return NewPINQueryable<T>(source.Except(other), new PINQAgentBinary(agent, otherAgent));
        }

        /// <summary>
        /// PINQ Except entry point. Passes control to the other PINQueryable.
        /// </summary>
        /// <param name="other">Other PINQueryable</param>
        /// <returns>Passes control to the other PINQueryable. Intends to return a PINQueryable containing all records except those in other.</returns>
        public PINQueryable<T> Except(PINQueryable<T> other)
        {
            return other.ExceptHelper(this);
        }

        /// <summary>
        /// Except helper. Passes control to other PINQueryable with an unprotected IQueryable.
        /// </summary>
        /// <param name="first">Other PINQueryable</param>
        /// <returns>Passes control to the other PINQueryable. Intends to return a PINQueryable containing all records in other, except records in this.</returns>
        public virtual PINQueryable<T> ExceptHelper(PINQueryable<T> first)
        {
            return first.Except(source, agent);
        }

        #endregion

        #endregion

        #region Partition Operation
        /// <summary>
        /// Partitions the PINQueryable into a set of PINQueryables, one for each of the provided keys.
        /// </summary>
        /// <typeparam name="K">Type of the keys.</typeparam>
        /// <param name="keys">Explicit set of possible key values.</param>
        /// <param name="keyFunc">Function that yields the key associated with a record.</param>
        /// <returns>An Dictionary mapping each key to a PINQueryable of records from the source data set that yield the key.</returns>
        public virtual Dictionary<K, PINQueryable<T>> Partition<K>(K[] keys, Expression<Func<T, K>> keyFunc)
        {
            keyFunc = rewrite(keyFunc) as Expression<Func<T, K>>;

            // the shared list of privicy increments for each key
            var agentTable = new Dictionary<K, double>();
            foreach (K k in keys.Distinct<K>())
                agentTable.Add(k, 0.0);

            // A common value to be shared by the PINQAgentPartitions (avoiding linear sweeps on epsilon increments).
            double[] maximum = new double[1] { 0.0 };

            // important to add keys independent of the data, as their order will be externally visible.
            var resultTable = new Dictionary<K, PINQueryable<T>>();
            foreach (K k in keys.Distinct())
                resultTable.Add(k, NewPINQueryable<T>(Enumerable.Empty<T>().AsQueryable(), new PINQAgentPartition<K>(agent, agentTable, k, maximum)));

            // GroupBy finds the associated parts for us.
            foreach (var group in source.GroupBy(keyFunc))
            {
                // only process keys we expect to find.
                if (resultTable.ContainsKey(group.Key))
                    resultTable[group.Key] = NewPINQueryable<T>(group.AsQueryable<T>(), new PINQAgentPartition<K>(agent, agentTable, group.Key, maximum));
            }

            return resultTable;
        }
        #endregion

        #region Constructors and Manipulation

        /// <summary>
        /// PINQueryable Constructor.
        /// </summary>
        /// <param name="s">Source IQueryable</param>
        /// <param name="a">Agent. null indicates no constraints on use.</param>
        /// <param name="r">Expression rewriter. null indicates no rewriting performed.</param>
        public PINQueryable(IQueryable<T> s, PINQAgent a, Func<Expression, Expression> r)
        {
            source = s;
            agent = (a == null) ? new PINQAgent() : a;
            rewrite = (r == null) ? exp => exp : r;
        }

        /// <summary>
        /// Constructor without an expression checker (all expressions are accepted).
        /// A null agent parameter introduces an agent that accepts all requests.
        /// </summary>
        /// <param name="s">Source IQueryable.</param>
        /// <param name="a">Agent. null indicates no constraints on use.</param>
        public PINQueryable(IQueryable<T> s, PINQAgent a)
        {
            source = s;
            agent = (a == null) ? new PINQAgent() : a;
            rewrite = exp => exp;
        }

        /// <summary>
        /// Factory method used to produce new PINQueryable objects in transformation methods. 
        /// Subtypes of PINQueryable should override this method to ensure the correct subtype of PINQueryable is created by each of their transformations.
        /// </summary>
        /// <typeparam name="S">Type of record underlying the IQueryable</typeparam>
        /// <param name="s">Source IQueryable</param>
        /// <param name="a">Agent</param>
        /// <returns></returns>
        public virtual PINQueryable<S> NewPINQueryable<S>(IQueryable<S> s, PINQAgent a)
        {
            return new PINQueryable<S>(s, a, rewrite);
        }

        /// <summary>
        /// Used to up-cast a more specialized PINQueryable
        /// </summary>
        /// <returns></returns>
        public virtual PINQueryable<T> AsPINQueryable()
        {
            return new PINQueryable<T>(source, agent, rewrite);
        }

        /// <summary>
        /// Materializes the contents of a PINQueryable. 
        /// In principle, this result in no information disclosure, but can be a useful performance 
        /// optimization for LINQ providers that are not good about spotting common subexpressions.
        /// </summary>
        /// <returns></returns>
        public virtual PINQueryable<T> Materialize()
        {
            return new PINQueryable<T>(source.ToArray().AsQueryable(), agent, rewrite);
        }

        #endregion
    };
}
