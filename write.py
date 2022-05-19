class Write:

    def write_data(self, data_frame, repartition_number, write_mode, write_format, path):

        data_frame.repartition(repartition_number).write.mode(write_mode).format(write_format).save(path)

