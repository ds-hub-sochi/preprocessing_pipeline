const prepareTask = async (task, code) => {
    const {premarkup, ...taskRest} = task
    const premarkupMarks = Array.isArray(premarkup ? premarkup.marks : null) ? premarkup.marks : [];
    return {
        ...taskRest,
        marks: premarkupMarks,
        validateResult(result) {
            if (result.marks.length === 0) {
                return 'Разметьте изображение';
            }
            return true;
        },

        transformResult(result) {
            return result;
        }
    };
};

exports.Task = class Task extends exports.Task {
    render() {
        super.render();
        prepareTask(this.task, this.code)
            .then((preparedTask) => {
                this.validate = preparedTask.validateResult;
                this.transform = preparedTask.transformResult;
                this.setPreparedTask(preparedTask);
            });
    }
};
