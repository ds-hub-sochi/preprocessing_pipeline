import numpy as np
import pandas as pd

_EPS: np.float64 = np.float_power(10, -10)


def get_most_probable_labels(proba: pd.DataFrame) -> pd.Series:
    """Returns most probable labels

    Args:
        proba (DataFrame): Tasks' label probability distributions.
            A pandas.DataFrame indexed by `task` such that `result.loc[task, label]`
            is the probability of `task`'s true label to be equal to `label`. Each
            probability is between 0 and 1, all task's probabilities should sum up to 1
    """
    if not proba.size:
        return pd.Series([], dtype='O')
    return proba.idxmax(axis='columns')


def normalize_rows(scores: pd.DataFrame) -> pd.DataFrame:
    """Scales values so that every raw sums to 1

    Args:
        scores (DataFrame): Tasks' label scores.
            A pandas.DataFrame indexed by `task` such that `result.loc[task, label]`
            is the score of `label` for `task`.

    Returns:
        DataFrame: Tasks' label probability distributions.
            A pandas.DataFrame indexed by `task` such that `result.loc[task, label]`
            is the probability of `task`'s true label to be equal to `label`. Each
            probability is between 0 and 1, all task's probabilities should sum up to 1
    """
    return scores.div(scores.sum(axis=1), axis=0)


class MajorityVote:
    def fit(
        self,
        data: pd.DataFrame,
    ):
        data = data[['subtask', 'marker_id', 'label']]

        scores = data[['subtask', 'label']].value_counts()

        self.probas_ = normalize_rows(scores.unstack('label', fill_value=0))
        self.labels_ = get_most_probable_labels(self.probas_)

        return self

    def fit_predict_proba(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        self.fit(data)

        return self.probas_

    def fit_predict(
        self,
        data: pd.DataFrame,
    ) -> pd.Series:
        self.fit(data)

        answer: pd.DataFrame = pd.DataFrame(self.labels_)
        answer = answer.rename(columns={0: 'aggregated_label'})

        return answer


class DawidSkene:
    def __init__(
        self,
        n_iter: int = 100,
        tol: float = 1e-5,
    ):
        self._n_iter: int = n_iter
        self._tol: float = tol

    @staticmethod
    def _m_step(
        data: pd.DataFrame,
        probas: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Performs M-step of the Dawid-Skene algorithm.
        Estimates the workers' error probability matrix using the specified workers'
        responses and the true task label probabilities.
        """
        joined = data.join(probas, on='subtask')
        joined.drop(columns=['subtask'], inplace=True)

        errors = joined.groupby(['marker_id', 'label'], sort=False).sum()
        errors.clip(lower=_EPS, inplace=True)
        errors /= errors.groupby('marker_id', sort=False).sum()

        return errors

    @staticmethod
    def _e_step(
        data: pd.DataFrame,
        priors: pd.Series,
        errors: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Performs E-step of the Dawid-Skene algorithm.
        Estimates the true task label probabilities using the specified workers' responses,
        the prior label probabilities, and the workers' error probability matrix.
        """

        # We have to multiply lots of probabilities and such products are known to converge
        # to zero exponentially fast. To avoid floating-point precision problems we work with
        # logs of original values
        joined = data.join(np.log2(errors), on=['marker_id', 'label'])  # type: ignore
        joined.drop(columns=['marker_id', 'label'], inplace=True)
        log_likelihoods = np.log2(priors) + joined.groupby('subtask', sort=False).sum()
        log_likelihoods.rename_axis('label', axis=1, inplace=True)

        # Exponentiating log_likelihoods 'as is' may still get us beyond our precision.
        # So we shift every row of log_likelihoods by a constant (which is equivalent to
        # multiplying likelihoods rows by a constant) so that max log_likelihood in each
        # row is equal to 0. This trick ensures proper scaling after exponentiating and
        # does not affect the result of E-step
        scaled_likelihoods = np.exp2(log_likelihoods.sub(log_likelihoods.max(axis=1), axis=0))
        scaled_likelihoods = scaled_likelihoods.div(scaled_likelihoods.sum(axis=1), axis=0)
        # Convert columns types to label type
        scaled_likelihoods.columns = pd.Index(scaled_likelihoods.columns, name='label', dtype=data.label.dtype)
        return scaled_likelihoods

    def _evidence_lower_bound(
        self,
        data: pd.DataFrame,
        probas: pd.DataFrame,
        priors: pd.Series,
        errors: pd.DataFrame,
    ) -> float:
        # calculate joint probability log-likelihood expectation over probas
        joined = data.join(np.log(errors), on=['marker_id', 'label'])  # type: ignore

        # escape boolean index/column names to prevent confusion between indexing by boolean array and iterable of names
        joined = joined.rename(columns={True: 'True', False: 'False'}, copy=False)
        priors = priors.rename(index={True: 'True', False: 'False'}, copy=False)

        joined.loc[:, priors.index] = joined.loc[:, priors.index].add(np.log(priors))  # type: ignore

        joined.set_index(['subtask', 'marker_id'], inplace=True)
        joint_expectation = (probas.rename(columns={True: 'True', False: 'False'}) * joined).sum().sum()

        entropy = -(np.log(probas) * probas).sum().sum()
        return float(joint_expectation + entropy)

    def fit(
        self,
        data: pd.DataFrame,
    ):
        """
        Fits the model to the training data with the EM algorithm.
        Args:
            data (DataFrame): The training dataset of workers' labeling results
                which is represented as the `pandas.DataFrame` data containing `task`, `worker`, and `label` columns.
        Returns:
            DawidSkene: self.
        """

        data = data[['subtask', 'marker_id', 'label']]

        # Early exit
        if not data.size:
            self.probas_ = pd.DataFrame()
            self.priors_ = pd.Series(dtype=float)
            self.errors_ = pd.DataFrame()
            self.labels_ = pd.Series(dtype=float)
            return self

        # Initialization
        probas = MajorityVote().fit_predict_proba(data)
        priors = probas.mean()
        errors = self._m_step(data, probas)
        loss = -np.inf
        self.loss_history_ = []

        # Updating proba and errors n_iter times
        for _ in range(self._n_iter):
            probas = self._e_step(data, priors, errors)
            priors = probas.mean()
            errors = self._m_step(data, probas)
            new_loss = self._evidence_lower_bound(data, probas, priors, errors) / len(data)
            self.loss_history_.append(new_loss)

            if new_loss - loss < self._tol:
                break
            loss = new_loss

        probas.columns = pd.Index(probas.columns, name='label', dtype=probas.columns.dtype)
        # Saving results
        self.probas_ = probas
        self.priors_ = priors
        self.errors_ = errors
        self.labels_ = get_most_probable_labels(probas)

        return self

    def fit_predict_proba(
        self,
        data: pd.DataFrame,
    ) -> pd.DataFrame:
        self.fit(data)

        return self.probas_

    def fit_predict(
        self,
        data: pd.DataFrame,
    ) -> pd.Series:
        self.fit(data)

        answer: pd.DataFrame = pd.DataFrame(self.labels_)
        answer = answer.rename(columns={'0': 'aggregated_label'})

        return answer
