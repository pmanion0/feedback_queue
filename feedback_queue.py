
from queue import PriorityQueue
from time import time

class FeedbackQueue:
    ''' A feedback queue '''

    def __init__(self, bake_gap = 20, expiry_gap = 200, batch_size = 100):
        ''' '''
        self.bake_gap = bake_gap
        self.expiry_gap = expiry_gap
        self.batch_size = batch_size
        #self.last_processed_time = time()
        self.negative_queue = []
        self.positive_queue = []
        self.positive_lookup = {}
        self.output_queue = []


    def has_baked_feedback(self):
        ''' Check if there is any baked feedback on either queue '''
        the_time = time()

        result = (self.queue_has_baked_feedback(the_time, self.negative_queue)
               or self.queue_has_baked_feedback(the_time, self.positive_queue) )

        return result

    def queue_has_baked_feedback(self, the_time, queue):
        ''' Check if there is any baked feedback on the given queue '''
        return len(queue) > 0 and self.is_baked(the_time, queue[0][0])


    def is_baked(self, the_time, data_time):
        ''' Determine whether a specific data timestamp is BAKED relative
            to the current reference time '''
        return data_time + self.bake_gap > the_time

    def is_unexpired(self, the_time, data_time):
        ''' Determine whether a specific data timestamp is EXPIRED relative
            to the current reference time '''
        return data_time + self.expiry_gap <= the_time


    def pop_one_valid_feedback(self, the_time, queue):
        ''' Remove the first baked data element from the given queue, and if
            it is not expired, add it to the output queue. Returns true if a
            baked element was found and false otherwise. This does not care
            whether the element was expired as this may indicate other
            baked but non-expired data is still waiting on the queue. '''
        if queue and self.is_baked(the_time, queue[0][0]):
            baked_feedback = queue.pop(0)
            if self.is_unexpired(the_time, baked_feedback[0]):
                self.output_queue.append(baked_feedback[1])
            return True
        else:
            return False

    def get_baked_feedback_batches(self):
        ''' Return an iterable of feedback batches of the requested batch size
            or batch size + 1 due to parallel processing of positive and
            negative feedback within a single pass '''
        the_time = time()
        read_positive = True
        read_negative = True

        while read_positive or read_negative:
            # Return an interable feedback list of the requested batch size
            if len(self.output_queue) >= self.batch_size:
                yield self.output_queue
                self.output_queue.clear()

            if read_positive:
                read_positive = self.pop_one_valid_feedback(the_time, self.positive_queue)
            if read_negative:
                read_negative = self.pop_one_valid_feedback(the_time, self.negative_queue)

    def purge_positive_lookup(self):
        ''' Remove all expired positive lookup data by recreating the
            dictionary lookup
                NOTE: **THIS MAY BE VERY TIME CONSUMING!** '''
        expired_timestamp = time() - self.expiry_gap

        self.positive_lookup = { k: v
            for k, v in self.positive_lookup.items()
            if v[0] < expired_timestamp # Only keep unexpired data
        }

    def positive_feedback(self, id):
        ''' Add the positive feedback for the given ID to the queue for future
            processing ensuring each feedback is only processed once '''
        if id in self.positive_lookup:
            feedback_tuple = self.positive_lookup.pop(id)
            self.queue_positive_feedback(feedback_tuple)
        else:
            print(f"{id} is invalid or feedback was already processsed")

    def queue_feedback(self, id, negative_feedback, positive_feedback):
        ''' Queue observations for later consumption '''
        the_time = time()
        self.queue_negative_feedback(the_time, negative_feedback)
        self.await_positive_feedback(the_time, id, positive_feedback)

    def queue_negative_feedback(self, timestamp, negative_feedback):
        ''' Add the negative feedback '''
        feedback_tuple = (timestamp, negative_feedback)
        self.negative_queue.append(feedback_tuple)

    def await_positive_feedback(self, timestamp, id, positive_feedback):
        ''' Create a '''
        feedback_tuple = (timestamp, positive_feedback)
        self.positive_lookup[id] = feedback_tuple
